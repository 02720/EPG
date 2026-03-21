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

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 1. 提速：全局初始化 OpenCC，避免每次调用时重复加载字典
cc = OpenCC("t2s")

# 2. 屏蔽网址的正则表达式
URL_PATTERN = re.compile(r'https?://[^\s<>"]+|www\.[^\s<>"]+')

def transform2_zh_hans(string):
    if not string:
        return ""
    return cc.convert(string)

def clean_text(text):
    """清除文本中的网址并去除首尾空格"""
    if not text:
        return ""
    text = URL_PATTERN.sub('', text)
    return text.strip()

def process_display_name(display_name):
    # 7. 去除“高清”，但不去除“超高清”
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

def parse_time(time_str):
    """3. 解决时间解析报错，兼容空值和无时区格式"""
    time_str = re.sub(r'\s+', '', time_str)
    if not time_str:
        return None
    # 如果没有时区信息（+或-），默认补全 +0800
    if '+' not in time_str and '-' not in time_str[8:]:
        time_str += '+0800'
    try:
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError as e:
        # 忽略无法解析的异常时间
        return None

def load_aliases(filepath='alias.txt'):
    """6. 加载别名配置，支持正则"""
    exact_aliases = {}
    regex_aliases = []
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                if len(parts) < 2:
                    continue
                main_name = parts[0].strip()
                for alias in parts[1:]:
                    alias = alias.strip()
                    if alias.startswith('re:'):
                        try:
                            # 编译正则表达式
                            regex_aliases.append((re.compile(alias[3:]), main_name))
                        except re.error:
                            print(f"无效的正则表达式: {alias}")
                    else:
                        exact_aliases[alias] = main_name
    return exact_aliases, regex_aliases

def get_main_name(name, exact_aliases, regex_aliases):
    """6. 根据别名获取主频道名"""
    if name in exact_aliases:
        return exact_aliases[name]
    for pattern, main_name in regex_aliases:
        if pattern.search(name):
            return main_name
    return name

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return url, gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return url, await response.text(encoding='utf-8')
    except Exception as e:
        print(f"[{url}] 请求失败: {e}")
    return url, None

def parse_epg(url, epg_content, exact_aliases, regex_aliases, epg_data):
    """解析单个EPG源，并将数据按 频道->日期->URL 归类"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError:
        return

    # 建立当前XML中 channel_id 到 主频道名 的映射
    xml_id_to_main_name = {}
    for channel in root.findall('channel'):
        c_id = channel.get('id')
        names = [dn.text for dn in channel.findall('display-name') if dn.text]
        if not names:
            names = [c_id]
        
        main_name = None
        for n in names:
            n = transform2_zh_hans(n)
            n = process_display_name(n)
            mapped_name = get_main_name(n, exact_aliases, regex_aliases)
            if main_name is None:
                main_name = mapped_name
        
        xml_id_to_main_name[c_id] = main_name

    # 解析节目单
    for prog in root.findall('programme'):
        c_id = prog.get('channel')
        main_name = xml_id_to_main_name.get(c_id)
        if not main_name:
            continue

        start_dt = parse_time(prog.get('start', ''))
        stop_dt = parse_time(prog.get('stop', ''))
        if not start_dt or not stop_dt:
            continue

        date_str = start_dt.strftime("%Y-%m-%d")

        # 4. 计算 title 长度，并清理文本
        title_len = 0
        for title in prog.findall('title'):
            t_text = clean_text(title.text)
            t_text = transform2_zh_hans(t_text) if t_text else "精彩节目"
            title.text = t_text
            title_len += len(t_text)
            
        for desc in prog.findall('desc'):
            d_text = clean_text(desc.text)
            d_text = transform2_zh_hans(d_text) if d_text else ""
            if d_text:
                desc.text = d_text
            else:
                prog.remove(desc) # 移除空的desc

        # 统一时间格式
        prog.set('start', start_dt.strftime("%Y%m%d%H%M%S %z"))
        prog.set('stop', stop_dt.strftime("%Y%m%d%H%M%S %z"))

        # 存入全局数据结构
        if url not in epg_data[main_name][date_str]:
            epg_data[main_name][date_str][url] = {'score': 0, 'programs': []}
        
        epg_data[main_name][date_str][url]['programs'].append(prog)
        epg_data[main_name][date_str][url]['score'] += title_len

def write_to_xml(final_channels, final_programs, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    for channel_name in sorted(final_channels):
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_name})
        ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"}).text = channel_name
        
        for prog in final_programs[channel_name]:
            prog.set('channel', channel_name)
            root.append(prog)

    # 1. 提速：使用 Python 3.9+ 原生的 indent 替代极慢的 minidom
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
    urls = get_urls()
    if not urls:
        return

    exact_aliases, regex_aliases = load_aliases()
    
    print("Fetching EPG data...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    # 数据结构: epg_data[频道名][日期][URL] = {'score': 标题总长度, 'programs': [节目列表]}
    epg_data = defaultdict(lambda: defaultdict(dict))
    
    print("Parsing EPG data...")
    for url, content in tqdm(results, desc="Parsing XMLs"):
        if content:
            parse_epg(url, content, exact_aliases, regex_aliases, epg_data)

    print("Merging EPG data...")
    final_programs = defaultdict(list)
    final_channels = set()
    
    # 5. 增加日志功能
    log_file = open('epg_source.log', 'w', encoding='utf-8')
    
    for channel_name, dates in tqdm(epg_data.items(), desc="Merging Channels"):
        final_channels.add(channel_name)
        for date_str, sources in dates.items():
            # 4. 比较同一频道一天内的节目信息，保留 title 总长度最长的来源
            best_url = max(sources.keys(), key=lambda url: sources[url]['score'])
            
            # 将选中的节目加入最终列表
            final_programs[channel_name].extend(sources[best_url]['programs'])
            
            # 写入日志
            log_file.write(f"频道: [{channel_name}] | 日期: {date_str} | 来源: {best_url}\n")
            
    log_file.close()

    print("Writing to XML...")
    write_to_xml(final_channels, final_programs, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("All Done!")

if __name__ == '__main__':
    asyncio.run(main())
