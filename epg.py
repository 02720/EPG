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
import logging

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 配置日志
logging.basicConfig(
    filename='epg_source.log',
    level=logging.INFO,
    format='%(message)s',
    encoding='utf-8'
)

# 预编译正则表达式
URL_PATTERN = re.compile(r'https?://[^\s]+')

def transform2_zh_hans(string):
    if not string:
        return ""
    cc = OpenCC("t2s")
    return cc.convert(string)

def process_display_name(display_name):
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

def clean_text(text):
    """清除文本中的网址"""
    if not text:
        return ""
    return URL_PATTERN.sub('', text).strip()

def parse_time(time_str):
    """健壮的时间解析函数，解决时间格式报错"""
    if not time_str:
        return None
    time_str = re.sub(r'\s+', '', time_str)
    if not time_str:
        return None
    
    # 如果缺少时区信息 (长度为14，如 20260301000600)，默认补全 +0800
    if len(time_str) == 14:
        time_str += '+0800'
        
    try:
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

def load_aliases(filepath='alias.txt'):
    """加载别名配置"""
    aliases_exact = {}
    aliases_re = []
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
                            # 支持正则，例如 re:(?i)^CCTV...
                            aliases_re.append((re.compile(alias[3:]), main_name))
                        except re.error as e:
                            print(f"正则表达式错误: {alias} -> {e}")
                    else:
                        aliases_exact[alias] = main_name
    return aliases_exact, aliases_re

def standardize_name(name, aliases_exact, aliases_re):
    """将别名标准化为主名"""
    if not name:
        return name
    if name in aliases_exact:
        return aliases_exact[name]
    for pattern, main_name in aliases_re:
        if pattern.match(name):
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
        print(f"\n[{url}] 请求失败: {e}")
    return url, None

def parse_epg(epg_content, source_url, aliases_exact, aliases_re):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        return {}, defaultdict(lambda: defaultdict(list)), defaultdict(lambda: defaultdict(int))

    channels = {}
    # programmes[channel_id][date_str] = [elements...]
    programmes = defaultdict(lambda: defaultdict(list))
    # title_lengths[channel_id][date_str] = total_length
    title_lengths = defaultdict(lambda: defaultdict(int))

    # 解析频道
    for channel in root.findall('channel'):
        raw_id = transform2_zh_hans(channel.get('id'))
        channel_id = standardize_name(raw_id, aliases_exact, aliases_re)
        
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            t_name = standardize_name(t_name, aliases_exact, aliases_re)
            channel_display_names.append((t_name, name.get('lang', 'zh')))
            
        if not channel_id.isdigit() and not any(channel_id == n[0] for n in channel_display_names):
            channel_display_names.append((channel_id, 'zh'))
            
        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    # 解析节目
    for programme in root.findall('programme'):
        raw_channel = transform2_zh_hans(programme.get('channel'))
        channel_id = standardize_name(raw_channel, aliases_exact, aliases_re)
        
        channel_start = parse_time(programme.get('start'))
        channel_stop = parse_time(programme.get('stop'))
        
        if not channel_start or not channel_stop:
            continue

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        date_str = channel_start.strftime("%Y-%m-%d")

        channel_elem = ET.Element('programme', attrib={
            "start": channel_start.strftime("%Y%m%d%H%M%S %z"), 
            "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
            "channel": channel_id
        })
        
        title_len_sum = 0
        
        for title in programme.findall('title'):
            channel_title = clean_text(title.text)
            if not channel_title:
                channel_title = "精彩节目"
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
                
            title_len_sum += len(channel_title)
            
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)
                
        for desc in programme.findall('desc'):
            channel_desc = clean_text(desc.text)
            if not channel_desc:
                continue
            langattr = desc.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(channel_elem, 'desc')
            channel_elem_d.text = channel_desc
            if langattr is not None:
                channel_elem_d.set('lang', langattr)
                
        programmes[channel_id][date_str].append(channel_elem)
        title_lengths[channel_id][date_str] += title_len_sum
        
    # 过滤掉今天没有节目的频道
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes, title_lengths

def indent_xml(elem, level=0):
    """原生 XML 缩进算法，比 minidom 快数十倍"""
    i = "\n" + level * "\t"
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "\t"
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for subelem in elem:
            indent_xml(subelem, level + 1)
        if not subelem.tail or not subelem.tail.strip():
            subelem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def write_to_xml(channels_names, final_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
        
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    for channel_id, display_names in channels_names.items():
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        # 去重 display-name
        seen_names = set()
        for display_name, langattr in display_names:
            if display_name not in seen_names:
                display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
                display_name_elem.text = display_name
                seen_names.add(display_name)
                
        for prog in final_programmes.get(channel_id, []):
            root.append(prog)

    # 使用原生算法美化 XML，极大提升速度
    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls

async def main():
    # 每次运行前清空旧日志
    if os.path.exists('epg_source.log'):
        os.remove('epg_source.log')
        
    urls = get_urls()
    if not urls:
        print("未找到 config.txt 或文件为空")
        return
        
    aliases_exact, aliases_re = load_aliases('alias.txt')
    
    tasks = [fetch_epg(url) for url in urls]
    print("正在并发获取 EPG 数据...")
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    all_channel_names = defaultdict(list)
    
    # 结构: all_programmes[channel_id][date_str][source_url] = [elements...]
    all_programmes = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    # 结构: all_title_lengths[channel_id][date_str][source_url] = length
    all_title_lengths = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    print("正在解析并合并 EPG 数据...")
    for source_url, epg_content in tqdm(results, desc="Parsing EPGs"):
        if not epg_content:
            continue
            
        channels, programmes, title_lengths = parse_epg(epg_content, source_url, aliases_exact, aliases_re)
        
        for channel_id, display_names in channels.items():
            all_channel_names[channel_id].extend(display_names)
            
            for date_str, progs in programmes[channel_id].items():
                all_programmes[channel_id][date_str][source_url] = progs
                all_title_lengths[channel_id][date_str][source_url] = title_lengths[channel_id][date_str]

    print("正在执行最优策略筛选...")
    final_programmes = defaultdict(list)
    
    # 按照 (频道, 日期) 筛选 title 总长度最长的来源
    for channel_id, dates in all_programmes.items():
        for date_str, sources in dates.items():
            # 找出当天 title 总长度最长的 source_url
            best_source = max(sources.keys(), key=lambda s: all_title_lengths[channel_id][date_str][s])
            
            # 记录日志
            channel_display = all_channel_names[channel_id][0][0] if all_channel_names[channel_id] else channel_id
            logging.info(f"频道: [{channel_display}] | 日期: {date_str} | 来源: {best_source}")
            
            # 将最优来源的节目加入最终列表
            final_programmes[channel_id].extend(sources[best_source])

    print("正在写入 XML 文件 (使用高速算法)...")
    write_to_xml(all_channel_names, final_programmes, 'output/epg.xml')
    
    print("正在压缩为 GZ 文件...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("全部完成！日志已保存至 epg_source.log")

if __name__ == '__main__':
    asyncio.run(main())
