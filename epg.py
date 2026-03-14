import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
import re
from opencc import OpenCC
import os
import logging
from tqdm import tqdm

# 配置日志
logging.basicConfig(
    filename='epg_source.log', 
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    encoding='utf-8'
)

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 1. 优化点：全局实例化 OpenCC，避免频繁实例化导致的极大性能损耗
cc = OpenCC("t2s")

# 2. 优化点：正则表达式，用于过滤域名和URL
URL_PATTERN = re.compile(
    r'(?:https?://)?(?:www\.)?[a-zA-Z0-9-]+\.(?:com|cn|net|org|tv|top|cc|me|pw|io|xyz)(?:/[a-zA-Z0-9-./?%&=]*)?',
    re.IGNORECASE
)

def transform2_zh_hans(string):
    if not string:
        return ""
    return cc.convert(string)

def remove_urls(text):
    if not text:
        return ""
    # 移除网址
    text = URL_PATTERN.sub('', text)
    # 清理可能残留的 "更多精彩尽在" 等前后缀及多余空格
    return text.strip()

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        # 增加超时限制，防止卡死
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers, timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return url, gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return url, await response.text(encoding='utf-8')
    except Exception as e:
        print(f"\n[{url}] 请求失败: {e}")
    return url, None

def process_display_name(display_name):
    if display_name and display_name.endswith('高清'):
        return display_name[:-2]
    return display_name

# 3. 优化点：安全的日期解析，解决各种由于格式不标准导致的 ValueError
def safe_parse_time(time_str):
    if not time_str:
        return None
    # 移除所有空白字符
    time_str = re.sub(r'\s+', '', time_str)
    if not time_str:
        return None
    # 如果缺失时区，补全 +0800
    if len(time_str) == 14:
        time_str += "+0800"
    # 兼容某些时区带冒号的情况如 +08:00
    time_str = time_str.replace(':', '')
    try:
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

def parse_epg(epg_content, source_url):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"\nXML解析错误 [{source_url}]: {e}")
        return {}, {}

    channels = {}
    
    # 解析频道信息
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = process_display_name(transform2_zh_hans(name.text))
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        
        # 兜底
        if not channel_id.isdigit() and not any(channel_id == n[0] for n in channel_display_names):
            channel_display_names.append([channel_id, 'zh'])
            
        channels[channel_id] = channel_display_names

    # programme 按 (频道ID) 和 (日期) 分组
    # 结构: programmes_by_day[channel_id][date_str] = {"score": 总标题长度, "progs": [ET.Element]}
    programmes_by_day = defaultdict(lambda: defaultdict(lambda: {"score": 0, "progs": []}))

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        
        channel_start = safe_parse_time(programme.get('start'))
        channel_stop = safe_parse_time(programme.get('stop'))
        
        if not channel_start or not channel_stop:
            continue

        prog_date = channel_start.date().strftime("%Y-%m-%d")

        channel_elem = ET.Element(
            'programme', 
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"), 
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
                "channel": channel_id
            }
        )
        
        daily_score = 0

        # 处理 title
        for title in programme.findall('title'):
            raw_title = title.text.strip() if title.text else "精彩节目"
            clean_title = remove_urls(raw_title)
            
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                clean_title = transform2_zh_hans(clean_title)
                
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = clean_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)
                
            daily_score += len(clean_title)

        # 处理 desc
        for desc in programme.findall('desc'):
            if not desc.text:
                continue
            clean_desc = remove_urls(desc.text.strip())
            if not clean_desc:
                continue
                
            langattr = desc.get('lang')
            if langattr == 'zh' or langattr is None:
                clean_desc = transform2_zh_hans(clean_desc)
                
            channel_elem_d = ET.SubElement(channel_elem, 'desc')
            channel_elem_d.text = clean_desc
            if langattr is not None:
                channel_elem_d.set('lang', langattr)

        programmes_by_day[channel_id][prog_date]["progs"].append(channel_elem)
        programmes_by_day[channel_id][prog_date]["score"] += daily_score

    return channels, programmes_by_day

def write_to_xml(channels_id, channels_names, final_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
        
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    # 写入 Channel
    for channel_id in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name, langattr in channels_names[channel_id]:
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
            
        # 写入 Programme
        for date_str, prog_data in sorted(final_programmes.get(channel_id, {}).items()):
            for prog in prog_data["progs"]:
                prog.set('channel', channel_id) 
                root.append(prog)

    # 1. 优化点：使用 ET.indent 代替 minidom（速度提升极速）
    if hasattr(ET, "indent"):
        ET.indent(root, space="\t", level=0)
    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        print("未找到 config.txt，请创建并填入URL。")
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

async def main():
    urls = get_urls()
    if not urls:
        return

    tasks = [fetch_epg(url) for url in urls]
    print("开始获取 EPG 数据...")
    # results 是包含 (url, content) 的列表
    results = await tqdm_asyncio.gather(*tasks, desc="下载进度")
    
    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    
    # final_programmes[map_id][date] = {"score": score, "source": url, "progs": progs}
    final_programmes = defaultdict(dict)

    print("开始解析和按天合并 EPG 数据...")
    
    for url, epg_content in tqdm(results, desc="处理数据源"):
        if not epg_content:
            continue
            
        channels, programmes_by_day = parse_epg(epg_content, url)
        
        for channel_id, display_names in channels.items():
            if channel_id not in programmes_by_day or not programmes_by_day[channel_id]:
                continue
                
            # 寻找统一频道ID (map_id)
            is_in_map = False
            map_id = channel_id
            for display_name_node in display_names:
                display_name = display_name_node[0]
                if display_name in all_channels_map:
                    is_in_map = True
                    map_id = all_channels_map[display_name]
                    break
            
            if not is_in_map:
                all_channel_id.add(map_id)
                all_channel_names[map_id] = display_names
                for display_name_node in display_names:
                    all_channels_map[display_name_node[0]] = map_id
            else:
                for display_name_node in display_names:
                    display_name = display_name_node[0]
                    if display_name not in all_channels_map:
                        all_channel_names[map_id].append(display_name_node)
                        all_channels_map[display_name] = map_id

            # 4. 优化点：按天比较，保留信息量（标题总长度）最多的
            for date_str, daily_data in programmes_by_day[channel_id].items():
                current_score = daily_data["score"]
                
                # 如果该频道在这一天还没有数据，或者新数据的分数更高，则覆盖
                if date_str not in final_programmes[map_id] or current_score > final_programmes[map_id][date_str]["score"]:
                    final_programmes[map_id][date_str] = {
                        "score": current_score,
                        "source": url,
                        "progs": daily_data["progs"]
                    }

    # 5. 优化点：记录最终日志
    logging.info("=========== 最终EPG数据源合并记录 ===========")
    for map_id in sorted(all_channel_id):
        for date_str, data in sorted(final_programmes[map_id].items()):
            # 记录: 频道名 - 日期 - 数据源URL
            ch_name = all_channel_names[map_id][0][0] if all_channel_names[map_id] else map_id
            logging.info(f"频道: [{ch_name}] | 日期: {date_str} | 来源: {data['source']}")

    print("正在生成 XML 文件...")
    write_to_xml(all_channel_id, all_channel_names, final_programmes, 'output/epg.xml')
    
    print("正在压缩 gz 文件...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    
    print("全部完成！详细合并来源记录已保存至 epg_source.log")

if __name__ == '__main__':
    asyncio.run(main())
