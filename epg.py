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

# 全局变量初始化，提升运行速度
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
CC_T2S = OpenCC("t2s")
URL_PATTERN = re.compile(r'http[s]?://\S+|www\.\S+')  # 匹配网址正则

def transform2_zh_hans(string):
    if not string:
        return ""
    return CC_T2S.convert(string)

def clean_text(text):
    """清理文本：移除网址广告并转换为简体"""
    if not text:
        return ""
    text = URL_PATTERN.sub('', text)  # 屏蔽网页链接
    return transform2_zh_hans(text).strip()

def parse_time(time_str):
    """强健的时间解析器，处理各种异常时间格式"""
    if not time_str:
        return None
    time_str = re.sub(r'\s+', '', time_str)
    try:
        # 判断是否包含时区 (例如：+0800 或 -0500)
        if len(time_str) >= 19 and ('+' in time_str[-5:] or '-' in time_str[-5:]):
            dt = datetime.strptime(time_str[:19], "%Y%m%d%H%M%S%z")
        else:
            # 不带时区，默认补齐为 UTC+8
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)
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
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    content = gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    content = await response.text(encoding='utf-8')
                return url, content  # 返回来源URL和内容
    except Exception as e:
        print(f"{url} 请求或读取失败: {e}")
    return url, None

def parse_epg(epg_content, source_url):
    channels = {}
    # 数据结构: programmes[channel_id][date] = {'length': 0, 'programs': [], 'source': source_url}
    programmes = defaultdict(lambda: defaultdict(lambda: {'length': 0, 'programs': [], 'source': source_url}))
    valid_channels = set()
    today = datetime.now(TZ_UTC_PLUS_8).date()

    try:
        # 使用流式解析 (XMLPullParser) 节省极大的内存并提升解析速度
        parser = ET.XMLPullParser(['end'])
        parser.feed(epg_content)
        for event, elem in parser.read_events():
            if elem.tag == 'channel':
                cid = transform2_zh_hans(elem.get('id', ''))
                names = []
                for name_elem in elem.findall('display-name'):
                    n = clean_text(name_elem.text)
                    if n.endswith('高清'):
                        n = n[:-2]
                    names.append((n, name_elem.get('lang', 'zh')))
                if not names:
                    names.append((cid, 'zh'))
                channels[cid] = names
                elem.clear()  # 释放内存

            elif elem.tag == 'programme':
                cid = transform2_zh_hans(elem.get('channel', ''))
                start_dt = parse_time(elem.get('start', ''))
                stop_dt = parse_time(elem.get('stop', ''))

                if not start_dt or not stop_dt:
                    elem.clear()
                    continue

                prog_date = start_dt.date()
                if stop_dt.date() == today:
                    valid_channels.add(cid)

                titles = []
                title_len = 0
                for t in elem.findall('title'):
                    t_text = clean_text(t.text) or "精彩节目"
                    titles.append({'text': t_text, 'lang': t.get('lang')})
                    title_len += len(t_text)

                descs = []
                for d in elem.findall('desc'):
                    d_text = clean_text(d.text)
                    if d_text:
                        descs.append({'text': d_text, 'lang': d.get('lang')})

                prog_data = {
                    'start': start_dt.strftime("%Y%m%d%H%M%S %z"),
                    'stop': stop_dt.strftime("%Y%m%d%H%M%S %z"),
                    'titles': titles,
                    'descs': descs
                }

                day_record = programmes[cid][prog_date]
                day_record['programs'].append(prog_data)
                day_record['length'] += title_len
                
                elem.clear()  # 释放内存
    except Exception as e:
        print(f"Error parsing XML from {source_url}: {e}")

    # 仅保留有效的频道
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def write_to_xml(unified_channels, unified_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    
    log_entries = []

    for uid, names in unified_channels.items():
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": uid})
        for n, lang in names:
            dn_elem = ET.SubElement(channel_elem, 'display-name')
            if lang: dn_elem.set('lang', lang)
            dn_elem.text = n

        # 按天遍历并组装 programmes
        for prog_date in sorted(unified_programmes[uid].keys()):
            day_data = unified_programmes[uid][prog_date]
            # 记录日志
            log_entries.append(f"[{prog_date}] 频道: {uid} | 数据源: {day_data['source']}")
            
            for p in day_data['programs']:
                p_elem = ET.SubElement(root, 'programme', attrib={
                    "start": p['start'],
                    "stop": p['stop'],
                    "channel": uid
                })
                for t in p['titles']:
                    t_elem = ET.SubElement(p_elem, 'title')
                    if t['lang']: t_elem.set('lang', t['lang'])
                    t_elem.text = t['text']
                for d in p['descs']:
                    d_elem = ET.SubElement(p_elem, 'desc')
                    if d['lang']: d_elem.set('lang', d['lang'])
                    d_elem.text = d['text']

    # 写入日志文件
    with open('output/epg_source.log', 'w', encoding='utf-8') as f:
        f.write("\n".join(log_entries))

    # 使用原生的格式化，速度远超 minidom (要求 Python 3.9+)
    if hasattr(ET, 'indent'):
        ET.indent(root, space='\t', level=0)
        
    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        print("未找到 config.txt，请检查配置。")
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
    print("正在下载 EPG 数据...")
    epg_results = await tqdm_asyncio.gather(*tasks, desc="下载进度")

    unified_channels = {}  # 频道统一ID -> 频道名称列表
    # unified_programmes[uid][date] = {'length': -1, 'programs': [], 'source': ''}
    unified_programmes = defaultdict(lambda: defaultdict(lambda: {'length': -1, 'programs': [], 'source': ''}))
    
    print("开始合并 EPG 数据...")
    for source_url, epg_content in tqdm(epg_results, desc="解析与合并", unit="file"):
        if not epg_content:
            continue
            
        channels, programmes = parse_epg(epg_content, source_url)
        
        for cid, names in channels.items():
            if not programmes[cid]:
                continue
                
            uid = None
            primary_name = names[0][0] if names else cid
            
            # 严格防止串台：按ID或首要名称去匹配
            if cid in unified_channels:
                uid = cid
            else:
                for existing_uid, existing_names in unified_channels.items():
                    if any(primary_name == en[0] for en in existing_names):
                        uid = existing_uid
                        break
                
            if not uid:
                uid = cid  # 创建新频道
                unified_channels[uid] = []
                
            # 添加别名
            for n in names:
                if n not in unified_channels[uid]:
                    unified_channels[uid].append(n)

            # 核心合并逻辑：以天为单位，保留当天 title 字符总数更多的源
            for prog_date, day_data in programmes[cid].items():
                existing_day_data = unified_programmes[uid][prog_date]
                if day_data['length'] > existing_day_data['length']:
                    unified_programmes[uid][prog_date] = day_data

    print("正在写入 XML 文件并生成日志...")
    write_to_xml(unified_channels, unified_programmes, 'output/epg.xml')
    print("正在压缩 gz 文件...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("全部完成！相关文件已存放在 output 目录中。")

if __name__ == '__main__':
    asyncio.run(main())
