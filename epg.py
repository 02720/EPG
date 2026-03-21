import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm
from typing import Dict, List, Tuple, Set, Optional
from functools import lru_cache

# ==================== 全局配置 ====================
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'epg_source.log')

# 预编译正则表达式（性能优化）
URL_PATTERN = re.compile(r'https?://[^\s<>"\']+', re.IGNORECASE)
WHITESPACE_PATTERN = re.compile(r'\s+')
# 匹配"高清"但不匹配"超高清"
HD_PATTERN = re.compile(r'(?<!超)高清$')

# 全局OpenCC转换器（复用提高性能）
_opencc_converter = None


def get_opencc():
    """获取OpenCC转换器单例"""
    global _opencc_converter
    if _opencc_converter is None:
        _opencc_converter = OpenCC("t2s")
    return _opencc_converter


def transform2_zh_hans(string: str) -> str:
    """繁体转简体"""
    if not string:
        return ""
    return get_opencc().convert(string)


# ==================== 频道别名处理 ====================
class ChannelAliasManager:
    """频道别名管理器"""
    
    def __init__(self):
        self.direct_aliases: Dict[str, str] = {}  # 直接别名映射
        self.regex_aliases: List[Tuple[re.Pattern, str]] = []  # 正则别名
        self._cache: Dict[str, str] = {}  # 缓存已解析的名称
    
    def load_from_file(self, filename: str = 'alias.txt'):
        """从文件加载别名配置"""
        filepath = os.path.join(SCRIPT_DIR, filename)
        
        if not os.path.exists(filepath):
            print(f"别名文件 {filename} 不存在，跳过加载")
            return
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = [p.strip() for p in line.split(',')]
                if len(parts) < 2:
                    continue
                
                primary_name = parts[0]
                # 主名也映射到自己
                self.direct_aliases[primary_name] = primary_name
                
                for alias in parts[1:]:
                    if alias.startswith('re:'):
                        # 正则表达式别名
                        pattern_str = alias[3:]
                        try:
                            compiled = re.compile(pattern_str)
                            self.regex_aliases.append((compiled, primary_name))
                        except re.error as e:
                            print(f"第{line_num}行正则表达式错误 '{pattern_str}': {e}")
                    else:
                        self.direct_aliases[alias] = primary_name
    
    def normalize(self, name: str) -> str:
        """将频道名称标准化为主名"""
        if not name:
            return name
        
        # 检查缓存
        if name in self._cache:
            return self._cache[name]
        
        # 直接别名匹配
        if name in self.direct_aliases:
            result = self.direct_aliases[name]
            self._cache[name] = result
            return result
        
        # 正则匹配
        for pattern, primary_name in self.regex_aliases:
            if pattern.search(name):
                self._cache[name] = primary_name
                return primary_name
        
        self._cache[name] = name
        return name
    
    def get_stats(self) -> Tuple[int, int]:
        """获取统计信息"""
        return len(self.direct_aliases), len(self.regex_aliases)


# ==================== EPG数据结构 ====================
class DailyProgrammes:
    """一天的节目数据"""
    __slots__ = ['programmes', 'total_title_length', 'source']
    
    def __init__(self):
        self.programmes: List[Tuple[ET.Element, int]] = []  # (节目元素, 标题长度)
        self.total_title_length: int = 0
        self.source: str = ""
    
    def update_if_better(self, new_programmes: List[Tuple[ET.Element, int]], 
                         new_total_length: int, source: str) -> bool:
        """如果新数据更好则更新，返回是否更新"""
        if new_total_length > self.total_title_length:
            self.programmes = new_programmes
            self.total_title_length = new_total_length
            self.source = source
            return True
        return False


# ==================== 工具函数 ====================
def process_display_name(display_name: str) -> str:
    """处理显示名称：去除'高清'但保留'超高清'"""
    if not display_name:
        return ""
    # 使用负向后视断言，只匹配不在"超"后面的"高清"
    return HD_PATTERN.sub('', display_name)


def filter_url_content(text: str) -> Optional[str]:
    """过滤文本中的URL"""
    if not text:
        return None
    filtered = URL_PATTERN.sub('', text).strip()
    return filtered if filtered else None


def parse_time_safely(time_str: str) -> Optional[datetime]:
    """安全解析时间字符串，支持多种格式"""
    if not time_str or not time_str.strip():
        return None
    
    # 移除空白字符
    time_str = WHITESPACE_PATTERN.sub('', time_str)
    
    # 基本长度检查
    if len(time_str) < 14:
        return None
    
    # 验证年份合理性
    try:
        year = int(time_str[:4])
        if year > 2100 or year < 1900:
            return None
    except ValueError:
        return None
    
    # 尝试带时区格式
    try:
        return datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
    except ValueError:
        pass
    
    # 尝试不带时区格式（假设为UTC+8）
    try:
        dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=TZ_UTC_PLUS_8)
    except ValueError:
        pass
    
    return None


# ==================== EPG获取和解析 ====================
async def fetch_epg(url: str, session: aiohttp.ClientSession) -> Tuple[Optional[str], str]:
    """获取EPG数据"""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status != 200:
                print(f"{url} HTTP错误: {response.status}")
                return None, url
            
            if url.endswith('.gz'):
                compressed_data = await response.read()
                return gzip.decompress(compressed_data).decode('utf-8', errors='ignore'), url
            else:
                return await response.text(encoding='utf-8'), url
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except gzip.BadGzipFile as e:
        print(f"{url} Gzip解压错误: {e}")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None, url


async def fetch_all_epgs(urls: List[str]) -> List[Tuple[Optional[str], str]]:
    """并发获取所有EPG"""
    connector = aiohttp.TCPConnector(limit=16, ssl=False, limit_per_host=4)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
        tasks = [fetch_epg(url, session) for url in urls]
        results = await tqdm_asyncio.gather(*tasks, desc="获取EPG数据")
    
    return results


def parse_epg(epg_content: str, source_url: str, 
              alias_manager: ChannelAliasManager) -> Tuple[Dict, Dict]:
    """
    解析EPG内容
    返回: (频道信息字典, 节目信息字典)
    节目信息按日期分组: {channel_id: {date_str: [(programme_elem, title_length), ...]}}
    """
    try:
        # 移除BOM
        if epg_content.startswith('\ufeff'):
            epg_content = epg_content[1:]
        
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content.encode('utf-8'), parser=parser)
    except ET.ParseError as e:
        print(f"XML解析错误 {source_url}: {e}")
        return {}, {}

    channels: Dict[str, List] = {}
    programmes: Dict[str, Dict[str, List]] = defaultdict(lambda: defaultdict(list))
    
    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels: Set[str] = set()
    channel_id_mapping: Dict[str, str] = {}  # 原始ID到标准化ID的映射

    # ===== 解析频道 =====
    for channel in root.findall('channel'):
        orig_channel_id = channel.get('id', '')
        if not orig_channel_id:
            continue
        
        orig_channel_id = transform2_zh_hans(orig_channel_id)
        display_names: List[List] = []
        
        for name_elem in channel.findall('display-name'):
            if name_elem.text is None:
                continue
            name_text = transform2_zh_hans(name_elem.text.strip())
            name_text = process_display_name(name_text)
            if name_text:
                display_names.append([name_text, name_elem.get('lang', 'zh')])
        
        # 如果ID不是纯数字，也作为显示名
        if orig_channel_id and not orig_channel_id.isdigit():
            processed_id = process_display_name(orig_channel_id)
            if processed_id and processed_id not in [n[0] for n in display_names]:
                display_names.append([processed_id, 'zh'])
        
        # 使用别名管理器标准化频道名称
        normalized_id = None
        for name_node in display_names:
            normalized = alias_manager.normalize(name_node[0])
            if normalized != name_node[0]:
                normalized_id = normalized
                break
        
        if normalized_id is None:
            normalized_id = alias_manager.normalize(orig_channel_id)
        
        channel_id_mapping[orig_channel_id] = normalized_id
        
        # 合并频道信息
        if normalized_id not in channels:
            channels[normalized_id] = display_names
        else:
            existing_names = {n[0] for n in channels[normalized_id]}
            for name_node in display_names:
                if name_node[0] not in existing_names:
                    channels[normalized_id].append(name_node)

    # ===== 解析节目 =====
    for programme in root.findall('programme'):
        orig_channel_id = transform2_zh_hans(programme.get('channel', ''))
        
        # 标准化频道ID
        if orig_channel_id in channel_id_mapping:
            channel_id = channel_id_mapping[orig_channel_id]
        else:
            channel_id = alias_manager.normalize(orig_channel_id)
        
        # 解析时间
        start_str = programme.get('start')
        stop_str = programme.get('stop')
        
        channel_start = parse_time_safely(start_str)
        channel_stop = parse_time_safely(stop_str)
        
        if channel_start is None or channel_stop is None:
            continue
        
        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)

        # 只保留今天及以后的节目
        if channel_stop.date() < today:
            continue
        
        valid_channels.add(channel_id)

        # 创建节目元素
        prog_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
            }
        )
        
        title_length = 0
        has_valid_title = False
        
        # 处理标题
        for title in programme.findall('title'):
            title_text = title.text
            if title_text is None:
                title_text = "精彩节目"
            else:
                title_text = title_text.strip()
                # 过滤URL
                filtered = filter_url_content(title_text)
                title_text = filtered if filtered else "精彩节目"
            
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                title_text = transform2_zh_hans(title_text)
            
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if langattr:
                title_elem.set('lang', langattr)
            
            title_length += len(title_text)
            has_valid_title = True
        
        # 确保有标题
        if not has_valid_title:
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = "精彩节目"
            title_length = 4
        
        # 处理描述
        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            desc_text = desc.text.strip()
            # 过滤URL
            desc_text = filter_url_content(desc_text)
            if not desc_text:
                continue
            
            langattr = desc.get('lang')
            if langattr == 'zh' or langattr is None:
                desc_text = transform2_zh_hans(desc_text)
            
            desc_elem = ET.SubElement(prog_elem, 'desc')
            desc_elem.text = desc_text
            if langattr:
                desc_elem.set('lang', langattr)
        
        # 按开始日期分组
        date_key = channel_start.strftime("%Y-%m-%d")
        programmes[channel_id][date_key].append((prog_elem, title_length))

    # 过滤无效频道
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: dict(v) for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


# ==================== 输出功能 ====================
def write_to_xml(channels_id: Set[str], channels_names: Dict, 
                 programmes: Dict, filename: str):
    """写入XML文件"""
    output_dir = os.path.join(SCRIPT_DIR, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    output_path = os.path.join(output_dir, os.path.basename(filename))
    
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    sorted_channels = sorted(channels_id)
    
    # 添加频道定义
    for channel_id in sorted_channels:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names.get(channel_id, []):
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            name_elem = ET.SubElement(
                channel_elem, 'display-name', attrib={"lang": langattr})
            name_elem.text = display_name
    
    # 添加节目，按频道和时间排序
    for channel_id in sorted_channels:
        channel_progs = programmes.get(channel_id, {})
        for date_key in sorted(channel_progs.keys()):
            daily_data = channel_progs[date_key]
            if isinstance(daily_data, DailyProgrammes):
                prog_list = daily_data.programmes
            else:
                prog_list = daily_data.get('programmes', [])
            
            # 按开始时间排序
            prog_list.sort(key=lambda x: x[0].get('start', ''))
            
            for prog, _ in prog_list:
                prog.set('channel', channel_id)
                root.append(prog)

    # 美化输出
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))
    
    return output_path


def compress_to_gz(input_filename: str, output_filename: str):
    """压缩为gzip格式"""
    output_dir = os.path.dirname(input_filename)
    output_path = os.path.join(output_dir, os.path.basename(output_filename))
    
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def write_source_log(channels_id: Set[str], channels_names: Dict, 
                     programmes: Dict, log_file: str):
    """写入来源日志"""
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"# EPG来源日志 - 生成时间: {datetime.now(TZ_UTC_PLUS_8).strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("# 格式: 频道: [频道名] | 日期: 年-月-日 | 来源: URL\n\n")
        
        for channel_id in sorted(channels_id):
            # 获取频道显示名
            names = channels_names.get(channel_id, [])
            channel_name = names[0][0] if names else channel_id
            
            channel_progs = programmes.get(channel_id, {})
            for date_key in sorted(channel_progs.keys()):
                daily_data = channel_progs[date_key]
                if isinstance(daily_data, DailyProgrammes):
                    source = daily_data.source
                else:
                    source = daily_data.get('source', 'unknown')
                
                f.write(f"频道: {channel_name} | 日期: {date_key} | 来源: {source}\n")


def get_urls() -> List[str]:
    """从配置文件读取URL列表"""
    urls = []
    config_path = os.path.join(SCRIPT_DIR, 'config.txt')
    
    if not os.path.exists(config_path):
        print(f"配置文件 {config_path} 不存在")
        return urls
    
    with open(config_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


# ==================== 主程序 ====================
async def main():
    print("=" * 50)
    print("EPG合并工具")
    print("=" * 50)
    
    # 加载URL配置
    urls = get_urls()
    if not urls:
        print("没有配置任何EPG源URL，请检查config.txt")
        return
    print(f"已加载 {len(urls)} 个EPG源")
    
    # 加载频道别名
    print("\n加载频道别名配置...")
    alias_manager = ChannelAliasManager()
    alias_manager.load_from_file('alias.txt')
    direct_count, regex_count = alias_manager.get_stats()
    print(f"已加载 {direct_count} 个直接别名, {regex_count} 个正则别名")
    
    # 获取EPG数据
    print("\n开始获取EPG数据...")
    epg_results = await fetch_all_epgs(urls)
    success_count = sum(1 for content, _ in epg_results if content is not None)
    print(f"成功获取 {success_count}/{len(urls)} 个EPG源")
    
    # 数据结构
    all_channels_map: Dict[str, str] = {}  # 显示名到主ID的映射
    all_channel_id: Set[str] = set()
    all_channel_names: Dict[str, List] = defaultdict(list)
    all_programmes: Dict[str, Dict[str, DailyProgrammes]] = defaultdict(
        lambda: defaultdict(DailyProgrammes)
    )
    
    # 处理每个EPG源
    print("\n开始合并EPG数据...")
    for i, (epg_content, source_url) in enumerate(epg_results):
        print(f"\n处理 [{i+1}/{len(epg_results)}]: {source_url}")
        
        if epg_content is None:
            print("  跳过（获取失败）")
            continue
        
        channels, programmes = parse_epg(epg_content, source_url, alias_manager)
        print(f"  解析到 {len(channels)} 个频道")
        
        if not channels:
            continue
        
        with tqdm(total=len(channels), desc="  合并频道", unit="个", leave=False) as pbar:
            for channel_id, display_names in channels.items():
                if channel_id not in programmes or not programmes[channel_id]:
                    pbar.update(1)
                    continue
                
                # 查找是否已存在该频道
                is_existing = False
                map_id = None
                
                for name_node in display_names:
                    display_name = name_node[0]
                    if display_name in all_channels_map:
                        is_existing = True
                        map_id = all_channels_map[display_name]
                        break
                
                if not is_existing:
                    map_id = channel_id
                    all_channel_id.add(map_id)
                    all_channel_names[map_id] = display_names.copy()
                    
                    # 初始化节目数据
                    for date_key, prog_list in programmes[channel_id].items():
                        total_length = sum(length for _, length in prog_list)
                        daily = all_programmes[map_id][date_key]
                        daily.programmes = prog_list.copy()
                        daily.total_title_length = total_length
                        daily.source = source_url
                    
                    # 注册所有显示名
                    for name_node in display_names:
                        all_channels_map[name_node[0]] = map_id
                else:
                    # 按日期比较标题总长度
                    for date_key, prog_list in programmes[channel_id].items():
                        new_total_length = sum(length for _, length in prog_list)
                        daily = all_programmes[map_id][date_key]
                        daily.update_if_better(prog_list.copy(), new_total_length, source_url)
                    
                    # 添加新的显示名
                    existing_names = {n[0] for n in all_channel_names[map_id]}
                    for name_node in display_names:
                        if name_node[0] not in existing_names:
                            all_channel_names[map_id].append(name_node)
                            existing_names.add(name_node[0])
                        if name_node[0] not in all_channels_map:
                            all_channels_map[name_node[0]] = map_id
                
                pbar.update(1)
    
    # 统计信息
    total_programmes = sum(
        len(daily.programmes) 
        for channel_progs in all_programmes.values() 
        for daily in channel_progs.values()
    )
    print(f"\n合并完成: {len(all_channel_id)} 个频道, {total_programmes} 条节目")
    
    # 写入来源日志
    print("\n写入来源日志...")
    write_source_log(all_channel_id, all_channel_names, all_programmes, LOG_FILE)
    print(f"日志已保存: {LOG_FILE}")
    
    # 写入XML
    print("\n写入XML文件...")
    xml_path = write_to_xml(all_channel_id, all_channel_names, 
                           all_programmes, 'epg.xml')
    print(f"XML已保存: {xml_path}")
    
    # 压缩
    print("压缩文件...")
    compress_to_gz(xml_path, 'epg.gz')
    print(f"压缩完成: {xml_path.replace('.xml', '.gz')}")
    
    print("\n" + "=" * 50)
    print("全部完成!")
    print("=" * 50)


if __name__ == '__main__':
    asyncio.run(main())
