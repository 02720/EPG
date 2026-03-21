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

def transform2_zh_hans(string):
    if not string:
        return ""
    cc = OpenCC("t2s")
    return cc.convert(string)

def process_display_name(display_name):
    # 去除“高清”，但保留“超高清”
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

def load_aliases(filename='alias.txt'):
    """加载别名规则"""
    aliases = []
    if not os.path.exists(filename):
        return aliases
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split(',')
            if len(parts) < 2:
                continue
            main_name = parts[0]
            exact_matches = set()
            regex_matches = []
            for p in parts[1:]:
                if p.startswith('re:'):
                    try:
                        regex_matches.append(re.compile(p[3:], re.IGNORECASE))
                    except re.error as e:
                        print(f"正则表达式错误: {p} - {e}")
                else:
                    exact_matches.add(p)
            aliases.append({'main': main_name, 'exacts': exact_matches, 'regexes': regex_matches})
    return aliases

def get_main_channel_name(name, aliases):
    """根据别名规则获取频道主名"""
    for rule in aliases:
        if name in rule['exacts']:
            return rule['main']
        for reg in rule['regexes']:
            if reg.search(name):
                return rule['main']
    return name

def parse_time(time_str):
    """鲁棒的时间解析函数，解决报错问题"""
    if not time_str:
        return None
    time_str = re.sub(r'\s+', '', time_str)
    if len(time_str) < 14:
        return None
    try:
        # 如果长度恰好14位，说明没有时区信息，手动加上时区
        if len(time_str) == 14:
            dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
        else:
            # 兼容带有 +0800 等时区的信息
            dt = datetime.strptime(time_str[:19], "%Y%m%d%H%M%S%z")
            return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

def is_spam_title(title):
    """判断标题是否包含推广网址"""
    if not title:
        return False
    title_lower = title.lower()
    if 'http://' in title_lower or 'https://' in title_lower:
        return True
    # 匹配常见的网址特征
    if re.search(r'[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+\.?', title_lower):
        if 'www.' in title_lower or '.com' in title_lower or '.xyz' in title_lower or '.net' in title_lower:
            return True
    return False

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

def parse_epg_data(epg_content, source_url, aliases):
    """将XML数据解析为Python字典，大幅提升处理速度"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except Exception as e:
        return {}, {}

    # channel_id 映射到 主频道名
    id_to_main_name = {}
    
    # 提取频道信息
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        
        display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            if t_name:
                display_names.append(t_name)
                
        # 确定基准名称并应用别名转换
        base_name = display_names[0] if display_names else channel_id
        main_name = get_main_channel_name(base_name, aliases)
        id_to_main_name[channel_id] = main_name

    # programmes_by_day[main_name][date_str] = [prog1, prog2, ...]
    programmes_by_day = defaultdict(lambda: defaultdict(list))
    
    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_main_names = set()

    # 提取节目信息
    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        if channel_id not in id_to_main_name:
            continue
            
        main_name = id_to_main_name[channel_id]
        
        start_time = parse_time(programme.get('start'))
        stop_time = parse_time(programme.get('stop'))
        if not start_time or not stop_time:
            continue

        if stop_time.date() == today:
            valid_main_names.add(main_name)

        title_elem = programme.find('title')
        title_text = title_elem.text.strip() if title_elem is not None and title_elem.text else "精彩节目"
        title_text = transform2_zh_hans(title_text)
        
        # 拦截推广URL
        if is_spam_title(title_text):
            continue

        desc_elem = programme.find('desc')
        desc_text = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else ""
        desc_text = transform2_zh_hans(desc_text)

        date_str = start_time.strftime("%Y-%m-%d")
        
        prog_data = {
            'start': start_time.strftime("%Y%m%d%H%M%S %z"),
            'stop': stop_time.strftime("%Y%m%d%H%M%S %z"),
            'title': title_text,
            'desc': desc_text
        }
        programmes_by_day[main_name][date_str].append(prog_data)

    # 过滤掉今天没有节目结束的频道
    filtered_programmes = {k: v for k, v in programmes_by_day.items() if k in valid_main_names}
    
    return filtered_programmes

def indent(elem, level=0):
    """高效的XML格式化缩进替代方案，取代极慢的minidom"""
    i = "\n" + level * "\t"
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "\t"
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for subelem in elem:
            indent(subelem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def write_to_xml_and_log(merged_programmes):
    if not os.path.exists('output'):
        os.makedirs('output')

    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    log_lines = []

    # 按频道生成 XML 节点并记录日志
    for channel_name, days_data in merged_programmes.items():
        # 添加 channel 节点
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_name})
        display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"})
        display_name_elem.text = channel_name

        # 添加 programme 节点
        for date_str, data in sorted(days_data.items()):
            source_url = data['source']
            log_lines.append(f"频道: [{channel_name}] | 日期: {date_str} | 来源: {source_url}\n")
            
            for prog in data['progs']:
                prog_elem = ET.SubElement(root, 'programme', attrib={
                    "start": prog['start'], 
                    "stop": prog['stop'], 
                    "channel": channel_name
                })
                title_elem = ET.SubElement(prog_elem, 'title', attrib={"lang": "zh"})
                title_elem.text = prog['title']
                
                if prog['desc']:
                    desc_elem = ET.SubElement(prog_elem, 'desc', attrib={"lang": "zh"})
                    desc_elem.text = prog['desc']

    # 格式化 XML 并写入
    indent(root)
    tree = ET.ElementTree(root)
    tree.write('output/epg.xml', encoding='utf-8', xml_declaration=True)

    # 写入日志
    with open('epg_source.log', 'w', encoding='utf-8') as f:
        f.writelines(log_lines)

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
    aliases = load_aliases('alias.txt')
    urls = get_urls()
    if not urls:
        return

    print("Fetching EPG data...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished Fetching.")

    # 最终合并的数据结构:
    # merged_data[channel_name][date_str] = {'source': url, 'title_len': int, 'progs': list}
    merged_data = defaultdict(lambda: defaultdict(dict))

    for source_url, epg_content in tqdm(results, desc="Processing & Merging EPG", unit="source"):
        if not epg_content:
            continue
            
        # 解析数据并获取字典
        channel_data = parse_epg_data(epg_content, source_url, aliases)
        
        # 按照“每天的title总长度”进行合并比较
        for channel_name, days_info in channel_data.items():
            for date_str, progs in days_info.items():
                # 计算该天该频道的title总长度
                current_title_len = sum(len(p['title']) for p in progs)
                
                existing = merged_data[channel_name].get(date_str)
                if not existing or current_title_len > existing['title_len']:
                    # 若不存在或当前的title信息更丰富，则覆盖
                    merged_data[channel_name][date_str] = {
                        'source': source_url,
                        'title_len': current_title_len,
                        'progs': progs
                    }

    print("Generating XML and Log files...")
    write_to_xml_and_log(merged_data)
    
    print("Compressing to gz...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    
    print("All tasks completed successfully!")

if __name__ == '__main__':
    asyncio.run(main())
