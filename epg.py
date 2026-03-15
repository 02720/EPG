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
from tqdm import tqdm
from functools import lru_cache

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 全局初始化 OpenCC，避免重复加载耗时
cc = OpenCC("t2s")

# 预编译正则表达式，提高匹配速度
URL_PATTERN = re.compile(r'https?://[^\s]+')
# 匹配空白字符，用于清理
SPACE_PATTERN = re.compile(r'\s+')

# 全局别名存储
aliases_exact = {}
aliases_re = []

def load_aliases():
    """加载别名配置 (Req 6)"""
    if not os.path.exists('alias.txt'):
        print("未找到 alias.txt，跳过别名加载。")
        return
    
    with open('alias.txt', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',') if p.strip()]
            if len(parts) < 2:
                continue
            primary = parts[0]
            for alias in parts[1:]:
                if alias.startswith('re:'):
                    try:
                        aliases_re.append((re.compile(alias[3:]), primary))
                    except re.error as e:
                        print(f"正则表达式错误: {alias} -> {e}")
                else:
                    aliases_exact[alias] = primary
    print(f"已加载 {len(aliases_exact)} 个精确别名和 {len(aliases_re)} 个正则别名。")

@lru_cache(maxsize=10000)
def transform2_zh_hans(string):
    if not string:
        return ""
    return cc.convert(string)

@lru_cache(maxsize=10000)
def process_display_name(display_name):
    """处理频道名称，去除高清但不去除超高清 (Req 7)"""
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

@lru_cache(maxsize=10000)
def normalize_channel_name(name):
    """综合处理频道名称：简繁转换 -> 去除高清 -> 别名匹配 (Req 6)"""
    name = transform2_zh_hans(name)
    name = process_display_name(name)
    
    # 1. 精确匹配
    if name in aliases_exact:
        return aliases_exact[name]
    
    # 2. 正则匹配
    for pattern, primary in aliases_re:
        if pattern.match(name):
            return primary
            
    return name

def clean_text(text):
    """清理文本中的网址 (Req 2)"""
    if not text:
        return ""
    # 移除网址
    text = URL_PATTERN.sub('', text).strip()
    return text

def parse_time(time_str):
    """健壮的时间解析 (Req 3)"""
    if not time_str:
        return None
    time_str = SPACE_PATTERN.sub('', time_str)
    if not time_str:
        return None
        
    try:
        if len(time_str) == 14:  # 格式: %Y%m%d%H%M%S (无时区)
            dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
        else:  # 包含时区
            dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
            return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                if response.status != 200:
                    return url, None
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return url, gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return url, await response.text(encoding='utf-8')
    except Exception as e:
        print(f"[{url}] 请求失败: {e}")
    return url, None

def parse_epg(epg_content, source_url):
    """解析单个EPG源，返回按天分组的节目数据"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError:
        return {}, {}

    # channel_id -> set of display names
    channels = defaultdict(set)
    
    for channel in root.findall('channel'):
        raw_id = channel.get('id')
        if not raw_id: continue
        
        channel_id = normalize_channel_name(raw_id)
        channels[channel_id].add(channel_id)
        
        for name in channel.findall('display-name'):
            if name.text:
                t_name = normalize_channel_name(name.text)
                channels[channel_id].add(t_name)

    # channel_id -> date -> list of programs
    daily_programs = defaultdict(lambda: defaultdict(list))
    # channel_id -> date -> total title length
    daily_title_length = defaultdict(lambda: defaultdict(int))

    for programme in root.findall('programme'):
        raw_channel = programme.get('channel')
        if not raw_channel: continue
        channel_id = normalize_channel_name(raw_channel)

        start_dt = parse_time(programme.get('start'))
        stop_dt = parse_time(programme.get('stop'))
        
        if not start_dt or not stop_dt:
            continue

        prog_date = start_dt.date()

        # 处理 Title
        title_elem = programme.find('title')
        title_text = title_elem.text if title_elem is not None else ""
        title_text = clean_text(transform2_zh_hans(title_text))
        if not title_text:
            title_text = "精彩节目"

        # 处理 Desc
        desc_elem = programme.find('desc')
        desc_text = desc_elem.text if desc_elem is not None else ""
        desc_text = clean_text(transform2_zh_hans(desc_text))

        prog_dict = {
            'start': start_dt.strftime("%Y%m%d%H%M%S %z"),
            'stop': stop_dt.strftime("%Y%m%d%H%M%S %z"),
            'title': title_text,
            'desc': desc_text
        }

        daily_programs[channel_id][prog_date].append(prog_dict)
        daily_title_length[channel_id][prog_date] += len(title_text)

    return channels, daily_programs, daily_title_length

def write_to_xml_and_log(global_channels, global_programs, filename, log_filename):
    """生成最终XML并写入日志 (Req 1, 5)"""
    if not os.path.exists('output'):
        os.makedirs('output')
        
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    log_lines = []
    log_lines.append(f"EPG 合成日志 - 生成时间: {current_time}\n")
    log_lines.append("="*50)

    # 排序频道ID以保证输出稳定
    for channel_id in sorted(global_channels.keys()):
        # 写入 Channel 节点
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name in sorted(global_channels[channel_id]):
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"})
            display_name_elem.text = display_name

        # 写入 Programme 节点并记录日志
        if channel_id in global_programs:
            log_lines.append(f"\n频道: {channel_id}")
            # 按日期排序
            for prog_date in sorted(global_programs[channel_id].keys()):
                day_data = global_programs[channel_id][prog_date]
                source_url = day_data['source']
                log_lines.append(f"  日期: {prog_date} | 来源: {source_url} | 节目数: {len(day_data['programs'])}")
                
                for prog in day_data['programs']:
                    prog_elem = ET.SubElement(root, 'programme', attrib={
                        "start": prog['start'],
                        "stop": prog['stop'],
                        "channel": channel_id
                    })
                    title_elem = ET.SubElement(prog_elem, 'title', attrib={"lang": "zh"})
                    title_elem.text = prog['title']
                    if prog['desc']:
                        desc_elem = ET.SubElement(prog_elem, 'desc', attrib={"lang": "zh"})
                        desc_elem.text = prog['desc']

    # 写入日志文件
    with open(log_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(log_lines))

    # 写入XML文件 (使用 ET.indent 替代极慢的 minidom)
    if hasattr(ET, 'indent'):
        ET.indent(root, space="\t", level=0)
    
    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        print("未找到 config.txt")
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

async def main():
    load_aliases()
    urls = get_urls()
    if not urls:
        return

    print("Fetching EPG data...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    # 全局数据结构
    global_channels = defaultdict(set)
    # channel_id -> date -> {'length': int, 'programs': list, 'source': str}
    global_programs = defaultdict(lambda: defaultdict(lambda: {'length': -1, 'programs': [], 'source': ''}))

    print("\nProcessing and Merging EPG data...")
    for source_url, epg_content in tqdm(results, desc="Parsing & Merging", unit="source"):
        if not epg_content:
            continue
            
        channels, daily_programs, daily_title_length = parse_epg(epg_content, source_url)
        
        # 合并频道名称
        for ch_id, names in channels.items():
            global_channels[ch_id].update(names)

        # 合并节目单 (Req 4: 按天比较 title 总长度)
        for ch_id, dates in daily_programs.items():
            for prog_date, progs in dates.items():
                current_len = daily_title_length[ch_id][prog_date]
                # 如果当前源的该天节目 title 总长度大于已保存的，则替换
                if current_len > global_programs[ch_id][prog_date]['length']:
                    global_programs[ch_id][prog_date] = {
                        'length': current_len,
                        'programs': progs,
                        'source': source_url
                    }

    print("\nWriting to XML and Log...")
    write_to_xml_and_log(global_channels, global_programs, 'output/epg.xml', 'output/epg_source.log')
    
    print("Compressing to GZ...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("All Done!")

if __name__ == '__main__':
    asyncio.run(main())
