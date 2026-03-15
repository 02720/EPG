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
import copy

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 全局OpenCC实例，提高性能
cc = OpenCC("t2s")

# 脚本目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def transform2_zh_hans(string):
    if string is None:
        return ""
    return cc.convert(string)


def load_aliases():
    """从alias.txt加载频道别名"""
    alias_file = os.path.join(SCRIPT_DIR, 'alias.txt')
    direct_aliases = {}  # alias -> main_name
    regex_aliases = []   # list of (compiled_regex, main_name)
    main_names = set()
    
    if not os.path.exists(alias_file):
        return direct_aliases, regex_aliases, main_names
    
    with open(alias_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 1:
                continue
            main_name = parts[0]
            main_names.add(main_name)
            direct_aliases[main_name] = main_name
            
            for alias in parts[1:]:
                if not alias:
                    continue
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        compiled = re.compile(pattern)
                        regex_aliases.append((compiled, main_name))
                    except re.error as e:
                        print(f"无效的正则表达式 '{pattern}': {e}")
                else:
                    direct_aliases[alias] = main_name
    
    return direct_aliases, regex_aliases, main_names


def normalize_channel_name(name, direct_aliases, regex_aliases):
    """将别名转换为主名"""
    if not name:
        return name
    
    # 先尝试直接匹配（更快）
    if name in direct_aliases:
        return direct_aliases[name]
    
    # 尝试正则匹配
    for pattern, main_name in regex_aliases:
        try:
            if pattern.fullmatch(name) or pattern.match(name):
                return main_name
        except Exception:
            continue
    
    return name


def is_blocked_content(text):
    """检查文本是否包含需要屏蔽的内容（如网址）"""
    if not text:
        return False
    # 匹配URL
    url_pattern = re.compile(r'https?://[^\s<>"\'{}|\\^`\[\]]+', re.IGNORECASE)
    if url_pattern.search(text):
        return True
    # 检查是否为纯网址形式的域名
    domain_pattern = re.compile(r'^[a-zA-Z0-9][-a-zA-Z0-9]*(\.[a-zA-Z0-9][-a-zA-Z0-9]*)+(/.*)?$')
    if domain_pattern.match(text.strip()):
        return True
    return False


async def fetch_epg(session, url):
    """获取EPG数据"""
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"{url} HTTP状态码: {response.status}")
                return None
            if url.endswith('.gz'):
                compressed_data = await response.read()
                return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
            else:
                return await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None


def process_display_name(display_name):
    """移除'高清'后缀但保留'超高清'"""
    if not display_name:
        return ""
    # 使用负向后顾断言，仅当"高清"前面不是"超"时才移除
    display_name = re.sub(r'(?<!超)高清$', '', display_name)
    return display_name.strip()


def parse_time_safe(time_str):
    """安全解析时间字符串，处理各种格式"""
    if not time_str:
        return None
    
    # 移除空白字符
    time_str = re.sub(r'\s+', '', time_str)
    
    if not time_str:
        return None
    
    # 尝试带时区解析
    try:
        return datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
    except ValueError:
        pass
    
    # 尝试不带时区解析，假定UTC+8
    try:
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=TZ_UTC_PLUS_8)
    except ValueError:
        pass
    
    # 尝试其他常见格式
    formats = ["%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
        except ValueError:
            continue
    
    return None


def parse_epg(epg_content, source_url, direct_aliases, regex_aliases):
    """解析EPG XML内容"""
    try:
        # 移除可能的BOM和空白
        epg_content = epg_content.lstrip('\ufeff').strip()
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"解析XML出错 {source_url}: {e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)
    channel_id_to_normalized = {}

    # 解析频道
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id', ''))
        if not channel_id:
            continue
            
        display_names = []
        for name in channel.findall('display-name'):
            if name.text is None:
                continue
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            if t_name:
                display_names.append([t_name, name.get('lang', 'zh')])
        
        # 如果channel_id不是纯数字，也作为display_name
        if not channel_id.isdigit():
            processed_id = process_display_name(channel_id)
            if processed_id and processed_id not in [n[0] for n in display_names]:
                display_names.append([processed_id, 'zh'])
        
        # 使用别名规范化频道ID
        normalized_id = normalize_channel_name(channel_id, direct_aliases, regex_aliases)
        
        # 也检查display_names是否有别名匹配
        for dn in display_names:
            norm_dn = normalize_channel_name(dn[0], direct_aliases, regex_aliases)
            if norm_dn != dn[0]:
                normalized_id = norm_dn
                break
        
        channel_id_to_normalized[channel_id] = normalized_id
        channels[channel_id] = {
            'normalized_id': normalized_id,
            'display_names': display_names
        }

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_original_ids = set()

    # 解析节目
    for programme in root.findall('programme'):
        original_channel_id = transform2_zh_hans(programme.get('channel', ''))
        if not original_channel_id:
            continue
        
        start_str = programme.get('start', '')
        stop_str = programme.get('stop', '')
        
        channel_start = parse_time_safe(start_str)
        channel_stop = parse_time_safe(stop_str)
        
        if channel_start is None or channel_stop is None:
            continue
        
        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)

        # 检查节目是否有效（结束时间在今天或之后）
        if channel_stop.date() >= today:
            valid_original_ids.add(original_channel_id)

        # 创建新的programme元素
        prog_elem = ET.Element('programme', attrib={
            "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
            "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
            "_source": source_url,
            "_date": channel_start.strftime("%Y-%m-%d")
        })
        
        has_valid_title = False
        total_title_text = ""
        
        for title in programme.findall('title'):
            title_text = title.text.strip() if title.text else ""
            
            # 跳过包含URL的标题
            if is_blocked_content(title_text):
                continue
            
            if not title_text:
                title_text = "精彩节目"
            
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                title_text = transform2_zh_hans(title_text)
            
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if langattr:
                title_elem.set('lang', langattr)
            has_valid_title = True
            total_title_text += title_text
        
        if not has_valid_title:
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = "精彩节目"
            total_title_text = "精彩节目"
        
        # 保存title总长度用于比较
        prog_elem.set('_title_len', str(len(total_title_text)))
        
        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            desc_text = desc.text.strip()
            
            # 跳过包含URL的描述
            if is_blocked_content(desc_text):
                continue
            
            if not desc_text:
                continue
            
            langattr = desc.get('lang')
            if langattr == 'zh' or langattr is None:
                desc_text = transform2_zh_hans(desc_text)
            
            desc_elem = ET.SubElement(prog_elem, 'desc')
            desc_elem.text = desc_text
            if langattr:
                desc_elem.set('lang', langattr)
        
        programmes[original_channel_id].append(prog_elem)

    # 过滤无效频道
    valid_channels = {k: v for k, v in channels.items() if k in valid_original_ids}
    valid_programmes = {k: v for k, v in programmes.items() if k in valid_original_ids}

    return valid_channels, valid_programmes


def get_programme_date(prog_elem):
    """获取节目的日期"""
    # 优先使用预存的日期
    date_str = prog_elem.get('_date')
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            pass
    
    start_str = prog_elem.get('start')
    if start_str:
        dt = parse_time_safe(start_str)
        if dt:
            return dt.astimezone(TZ_UTC_PLUS_8).date()
    return None


def group_programmes_by_date(programmes):
    """按日期分组节目"""
    by_date = defaultdict(list)
    for prog in programmes:
        date = get_programme_date(prog)
        if date:
            by_date[date].append(prog)
    return dict(by_date)


def calculate_daily_title_length(programmes_by_date):
    """计算每日title总长度"""
    daily_lengths = {}
    for date, progs in programmes_by_date.items():
        total_len = 0
        for prog in progs:
            # 使用预存的title长度
            title_len = prog.get('_title_len')
            if title_len:
                total_len += int(title_len)
            else:
                for title in prog.findall('title'):
                    if title.text:
                        total_len += len(title.text)
        daily_lengths[date] = total_len
    return daily_lengths


def write_to_xml(channels_id, channels_names, programmes, filename):
    """写入XML文件"""
    output_dir = os.path.join(SCRIPT_DIR, 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    # 排序频道ID
    sorted_channels = sorted(channels_id)
    
    for channel_id in sorted_channels:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(
                channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
    
    # 添加节目，按时间排序
    for channel_id in sorted_channels:
        progs = sorted(programmes[channel_id], key=lambda p: p.get('start', ''))
        for prog in progs:
            # 创建副本并移除内部属性
            new_prog = ET.Element('programme')
            for key, value in prog.attrib.items():
                if not key.startswith('_'):
                    new_prog.set(key, value)
            new_prog.set('channel', channel_id)
            for child in prog:
                new_prog.append(copy.deepcopy(child))
            root.append(new_prog)

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    
    output_file = os.path.join(output_dir, filename)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))
    
    return output_file


def compress_to_gz(input_filename, output_filename):
    """压缩为gzip"""
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    """从config.txt读取URL列表"""
    config_file = os.path.join(SCRIPT_DIR, 'config.txt')
    urls = []
    if not os.path.exists(config_file):
        print(f"配置文件不存在: {config_file}")
        return urls
    
    with open(config_file, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


def init_log_file():
    """初始化日志文件"""
    log_file = os.path.join(SCRIPT_DIR, 'epg_source.log')
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"EPG来源日志 - 生成时间: {datetime.now(TZ_UTC_PLUS_8).strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
    return log_file


def write_final_log(log_file, all_programmes, channel_display_name_map, source_maps):
    """写入最终的来源日志"""
    with open(log_file, 'a', encoding='utf-8') as f:
        # 按频道名排序
        sorted_channels = sorted(source_maps.keys(), key=lambda x: channel_display_name_map.get(x, x))
        
        for channel_id in sorted_channels:
            channel_name = channel_display_name_map.get(channel_id, channel_id)
            date_sources = source_maps[channel_id]
            
            # 按日期排序
            for date in sorted(date_sources.keys()):
                source_url = date_sources[date]
                f.write(f"频道: [{channel_name}] | 日期: {date.strftime('%Y-%m-%d')} | 来源: {source_url}\n")


async def main():
    # 初始化日志文件
    log_file = init_log_file()
    
    # 加载别名
    print("加载频道别名...")
    direct_aliases, regex_aliases, main_names = load_aliases()
    print(f"已加载 {len(direct_aliases)} 个直接别名和 {len(regex_aliases)} 个正则表达式模式")
    
    # 获取URL列表
    urls = get_urls()
    if not urls:
        print("config.txt中没有找到URL")
        return
    
    print(f"共 {len(urls)} 个EPG源")
    
    # 创建共享的session以提高性能
    connector = aiohttp.TCPConnector(limit=16, ssl=False, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=60, connect=15)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout, headers=headers, trust_env=True) as session:
        # 并发获取EPG数据
        print("获取EPG数据...")
        tasks = [fetch_epg(session, url) for url in urls]
        epg_contents = await tqdm_asyncio.gather(*tasks, desc="获取URL")
    
    print("获取完成。")
    
    # 合并后的数据结构
    all_channels_map = {}          # display_name -> channel_id
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)
    all_source_maps = defaultdict(dict)    # channel_id -> {date: source_url}
    all_daily_lengths = defaultdict(dict)  # channel_id -> {date: total_title_length}
    channel_display_name_map = {}          # channel_id -> primary display name
    
    # 处理每个EPG源
    for i, epg_content in enumerate(epg_contents):
        source_url = urls[i]
        print(f"\n处理EPG源 {i+1}/{len(epg_contents)}: {source_url}")
        
        if epg_content is None:
            print("  跳过（无内容）")
            continue
        
        print("  解析EPG数据...")
        channels, programmes = parse_epg(epg_content, source_url, direct_aliases, regex_aliases)
        total_progs = sum(len(p) for p in programmes.values())
        print(f"  找到 {len(channels)} 个频道，{total_progs} 个节目")
        
        with tqdm(total=len(channels), desc="  合并EPG", unit="频道") as pbar:
            for original_id, channel_info in channels.items():
                normalized_id = channel_info['normalized_id']
                display_names = channel_info['display_names']
                
                if len(programmes[original_id]) == 0:
                    pbar.update(1)
                    continue
                
                # 查找是否已存在该频道
                existing_id = None
                
                # 先检查normalized_id
                if normalized_id in all_channels_map:
                    existing_id = all_channels_map[normalized_id]
                
                # 再检查display_names
                if existing_id is None:
                    for dn in display_names:
                        dn_name = dn[0]
                        if dn_name in all_channels_map:
                            existing_id = all_channels_map[dn_name]
                            break
                        norm_name = normalize_channel_name(dn_name, direct_aliases, regex_aliases)
                        if norm_name in all_channels_map:
                            existing_id = all_channels_map[norm_name]
                            break
                
                # 确定使用的频道ID
                map_id = existing_id if existing_id else normalized_id
                
                # 获取用于日志的显示名称
                primary_display_name = display_names[0][0] if display_names else map_id
                
                new_progs = programmes[original_id]
                new_by_date = group_programmes_by_date(new_progs)
                new_daily_lengths = calculate_daily_title_length(new_by_date)
                
                if existing_id is None:
                    # 新频道
                    all_channel_id.add(map_id)
                    all_channel_names[map_id] = display_names.copy()
                    channel_display_name_map[map_id] = primary_display_name
                    
                    # 添加所有节目和来源
                    for date, progs in new_by_date.items():
                        all_programmes[map_id].extend(progs)
                        all_source_maps[map_id][date] = source_url
                        all_daily_lengths[map_id][date] = new_daily_lengths.get(date, 0)
                    
                    # 注册所有名称
                    for dn in display_names:
                        all_channels_map[dn[0]] = map_id
                    all_channels_map[normalized_id] = map_id
                    all_channels_map[map_id] = map_id
                else:
                    # 已存在频道 - 按日期比较title总长度
                    existing_by_date = group_programmes_by_date(all_programmes[map_id])
                    
                    # 合并
                    merged_progs = []
                    all_dates = set(existing_by_date.keys()) | set(new_by_date.keys())
                    
                    for date in all_dates:
                        existing_len = all_daily_lengths[map_id].get(date, 0)
                        new_len = new_daily_lengths.get(date, 0)
                        
                        if new_len > existing_len:
                            # 使用新源
                            merged_progs.extend(new_by_date.get(date, []))
                            all_source_maps[map_id][date] = source_url
                            all_daily_lengths[map_id][date] = new_len
                        else:
                            # 保留现有
                            merged_progs.extend(existing_by_date.get(date, []))
                    
                    all_programmes[map_id] = merged_progs
                    
                    # 添加新的display names
                    existing_names = {dn[0] for dn in all_channel_names[map_id]}
                    for dn in display_names:
                        if dn[0] not in existing_names:
                            all_channel_names[map_id].append(dn)
                            all_channels_map[dn[0]] = map_id
                
                pbar.update(1)
    
    print(f"\n总频道数: {len(all_channel_id)}")
    print(f"总节目数: {sum(len(p) for p in all_programmes.values())}")
    
    # 写入最终日志
    print("\n写入来源日志...")
    write_final_log(log_file, all_programmes, channel_display_name_map, all_source_maps)
    
    print("写入XML...")
    output_file = write_to_xml(all_channel_id, all_channel_names, all_programmes, 'epg.xml')
    
    gz_file = output_file.replace('.xml', '.gz')
    compress_to_gz(output_file, gz_file)
    
    print(f"\n输出文件: {output_file}")
    print(f"压缩文件: {gz_file}")
    print(f"日志文件: {log_file}")
    print("完成！")


if __name__ == '__main__':
    asyncio.run(main())
