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

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 1. 全局初始化 OpenCC，极大提升速度
cc = OpenCC("t2s")

# 5. 配置日志记录器
logger = logging.getLogger("epg_logger")
logger.setLevel(logging.INFO)
log_file = 'epg_source.log'
# 每次运行清空旧日志
if os.path.exists(log_file):
    os.remove(log_file)
fh = logging.FileHandler(log_file, encoding='utf-8')
fh.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(fh)

def transform2_zh_hans(string):
    if not string:
        return ""
    return cc.convert(string)

# 2. 清理文本，屏蔽网址
def clean_text(text):
    if not text:
        return ""
    # 剔除 http/https 开头的网址
    text = re.sub(r'https?://[a-zA-Z0-9\.\-\/_\?\&\%=]+', '', text, flags=re.IGNORECASE)
    return transform2_zh_hans(text.strip())

# 3. 稳健的时间解析器，解决各类 ValueError
def parse_epg_datetime(time_str):
    if not time_str:
        return None
    time_str = time_str.strip()
    # 匹配14位数字，可选的空格和时区(如 +0800, +08:00)
    match = re.match(r'^(\d{14})\s*([+-]\d{2}:?\d{2})?', time_str)
    if not match:
        return None
    dt_str = match.group(1)
    tz_str = match.group(2)
    
    if tz_str:
        tz_str = tz_str.replace(':', '')  # 统一为 +0800 格式
    else:
        tz_str = '+0800'  # 缺失时区则默认补全 +0800
        
    try:
        dt = datetime.strptime(dt_str + tz_str, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

# 6. 加载别名配置
def load_aliases():
    aliases_exact = {}
    aliases_re = []
    if os.path.exists('alias.txt'):
        with open('alias.txt', 'r', encoding='utf-8') as f:
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
                            aliases_re.append((re.compile(alias[3:]), main_name))
                        except Exception as e:
                            print(f"Regex error in alias.txt: {alias} -> {e}")
                    else:
                        aliases_exact[alias] = main_name
    return aliases_exact, aliases_re

ALIASES_EXACT, ALIASES_RE = load_aliases()

def get_main_name(name):
    if not name: return ""
    # 1. 精确匹配
    if name in ALIASES_EXACT:
        return ALIASES_EXACT[name]
    # 2. 正则匹配
    for pattern, main_name in ALIASES_RE:
        if pattern.search(name):
            return main_name
    return name

def process_display_name(display_name):
    if display_name.endswith('高清'):
        display_name = display_name[:-2]
    return display_name

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
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
        print(f"\n{url} 请求失败: {e}")
    return url, None

def parse_epg(epg_content, source_url):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML from {source_url}: {e}")
        return {}

    local_channel_map = {} # raw_id -> main_name
    
    # 解析频道映射
    for channel in root.findall('channel'):
        raw_id = channel.get('id')
        names = [clean_text(n.text) for n in channel.findall('display-name') if n.text]
        
        main_name = None
        for n in names:
            processed_n = process_display_name(n)
            mapped = get_main_name(processed_n)
            if mapped != processed_n or main_name is None:
                main_name = mapped
                
        if not main_name:
            main_name = get_main_name(process_display_name(clean_text(raw_id)))
            
        local_channel_map[raw_id] = main_name

    # programmes_dict: main_name -> date_str -> list of programs
    programmes_dict = defaultdict(lambda: defaultdict(list))

    for programme in root.findall('programme'):
        raw_channel = programme.get('channel')
        main_name = local_channel_map.get(raw_channel)
        if not main_name:
            continue

        start_dt = parse_epg_datetime(programme.get('start'))
        stop_dt = parse_epg_datetime(programme.get('stop'))
        
        if not start_dt or not stop_dt:
            continue

        date_str = start_dt.date().isoformat()
        
        # 提取标题和描述，清理格式
        titles = []
        for t in programme.findall('title'):
            if t.text:
                titles.append({'text': clean_text(t.text), 'lang': t.get('lang', 'zh')})
        if not titles:
            titles.append({'text': "精彩节目", 'lang': 'zh'})
            
        descs = []
        for d in programme.findall('desc'):
            if d.text:
                descs.append({'text': clean_text(d.text), 'lang': d.get('lang', 'zh')})

        programmes_dict[main_name][date_str].append({
            'start': start_dt.strftime("%Y%m%d%H%M%S %z"),
            'stop': stop_dt.strftime("%Y%m%d%H%M%S %z"),
            'titles': titles,
            'descs': descs
        })

    return programmes_dict

def write_to_xml(final_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    # 为了保证 XML 的整洁和顺序，对频道进行排序
    for main_name in sorted(final_programmes.keys()):
        progs = final_programmes[main_name]
        if not progs:
            continue
            
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": main_name})
        display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"})
        display_name_elem.text = main_name

        for p in progs:
            prog_elem = ET.SubElement(root, 'programme', attrib={
                "start": p['start'], 
                "stop": p['stop'], 
                "channel": main_name
            })
            for t in p['titles']:
                t_elem = ET.SubElement(prog_elem, 'title', attrib={"lang": t['lang']})
                t_elem.text = t['text']
            for d in p['descs']:
                d_elem = ET.SubElement(prog_elem, 'desc', attrib={"lang": d['lang']})
                d_elem.text = d['text']

    # 使用 ET自带的缩进，抛弃缓慢的 minidom
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
        return []
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

    print("Fetching EPG data...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    # 4. 采用全新的按天合并数据结构: 频道 -> 日期 -> 来源 URL -> 节目列表
    global_programmes = defaultdict(lambda: defaultdict(dict))

    print("\nParsing EPG data...")
    for url, content in tqdm(results, desc="Parsing Sources", unit="file"):
        if not content:
            continue
        parsed_data = parse_epg(content, url)
        for main_name, dates_dict in parsed_data.items():
            for date_str, progs in dates_dict.items():
                global_programmes[main_name][date_str][url] = progs

    print("\nMerging EPG data (By title length per day)...")
    final_programmes = defaultdict(list)
    
    # 根据天为单位，比较 Title 长度进行留存
    for main_name, dates_dict in tqdm(global_programmes.items(), desc="Merging", unit="channel"):
        for date_str, sources in dates_dict.items():
            best_source = None
            max_title_len = -1
            best_progs = []

            for source_url, progs in sources.items():
                # 计算该源当天该频道所有节目的 title 总长度
                title_len = sum(len(t['text']) for p in progs for t in p['titles'])
                if title_len > max_title_len:
                    max_title_len = title_len
                    best_source = source_url
                    best_progs = progs
            
            # 加入最终队列并记录日志
            if best_progs:
                final_programmes[main_name].extend(best_progs)
                logger.info(f"频道: {main_name.ljust(15)} | 日期: {date_str} | 字符数: {str(max_title_len).ljust(5)} | 采用源: {best_source}")

    print("\nWriting to XML...")
    write_to_xml(final_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("All tasks completed! Check output/epg.xml and epg_source.log.")

if __name__ == '__main__':
    asyncio.run(main())
