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

# 全局初始化 OpenCC 和 正则表达式，避免循环中重复初始化导致性能暴跌
CC_T2S = OpenCC("t2s")
URL_PATTERN = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

def transform2_zh_hans(string):
    if not string:
        return ""
    return CC_T2S.convert(string)

def clean_text_and_url(text):
    """过滤文本中的网址/非法内容"""
    if not text:
        return ""
    text = URL_PATTERN.sub('', text)
    return text.strip()

def process_display_name(display_name):
    """去除高清，保留超高清"""
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

def parse_time(time_str):
    """自适应时间格式解析"""
    if not time_str:
        return None
    time_str = re.sub(r'\s+', '', time_str)
    try:
        # 如果长度大于等于19，或者包含了时区符号
        if len(time_str) >= 19 or '+' in time_str or '-' in time_str[14:]:
            dt = datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
        else:
            dt = datetime.strptime(time_str[:14], "%Y%m%d%H%M%S")
            dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None

def load_aliases(filepath='alias.txt'):
    """加载别名配置"""
    exact_aliases = {}
    re_aliases = []
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                if len(parts) >= 2:
                    main_name = parts[0].strip()
                    for alias in parts[1:]:
                        alias = alias.strip()
                        if alias.startswith('re:'):
                            try:
                                # 忽略大小写的正则编译
                                re_aliases.append((re.compile(alias[3:]), main_name))
                            except Exception as e:
                                print(f"正则表达式错误: {alias} -> {e}")
                        else:
                            exact_aliases[alias] = main_name
    return exact_aliases, re_aliases

def get_main_name(name, exact_aliases, re_aliases):
    """匹配主名，匹配不到则返回原名"""
    if name in exact_aliases:
        return exact_aliases[name]
    for pattern, main_name in re_aliases:
        if pattern.search(name):
            return main_name
    return None

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url, timeout=30) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore'), url
                else:
                    text = await response.text(encoding='utf-8')
                    return text, url
    except Exception as e:
        print(f"\n[{url}] 请求失败: {e}")
    return None, url

def parse_epg(epg_content, source_url, exact_aliases, re_aliases):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"\nXML解析错误 [{source_url}]: {e}")
        return {}, {}

    channels = {}
    channel_id_map = {}  # 记录原始 channel id 与标准 MainName 的映射关系

    # 解析频道 (Channel)
    for channel in root.findall('channel'):
        raw_id = channel.get('id')
        parsed_id = transform2_zh_hans(raw_id)
        
        display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            display_names.append((t_name, name.get('lang', 'zh')))
            
        if not parsed_id.isdigit() and not any(parsed_id == n[0] for n in display_names):
            display_names.append((parsed_id, 'zh'))

        # Alias: 优先尝试从 display_names 匹配标准主名，若没有再试 channel_id
        main_name = None
        for d_name, _ in display_names:
            matched = get_main_name(d_name, exact_aliases, re_aliases)
            if matched:
                main_name = matched
                break
        if not main_name:
            matched = get_main_name(parsed_id, exact_aliases, re_aliases)
            if matched:
                main_name = matched

        # 确定最终标准化名称
        final_id = main_name if main_name else parsed_id
        channel_id_map[raw_id] = final_id

        if final_id not in channels:
            channels[final_id] = set()
        for d in display_names:
            channels[final_id].add(d)

    # 存储每日节目，格式：programmes[channel_id][date_str] = {"score": int, "source": url, "progs": [dict, ...]}
    programmes = defaultdict(lambda: defaultdict(lambda: {"score": 0, "source": source_url, "progs": []}))
    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    # 解析节目单 (Programme)
    for programme in root.findall('programme'):
        raw_chan_id = programme.get('channel')
        channel_id = channel_id_map.get(raw_chan_id, transform2_zh_hans(raw_chan_id))

        start_dt = parse_time(programme.get('start'))
        stop_dt = parse_time(programme.get('stop'))

        if not start_dt or not stop_dt:
            continue

        if stop_dt.date() == today:
            valid_channels.add(channel_id)

        date_str = start_dt.date().strftime("%Y-%m-%d")

        # 提取并清理 Title
        title_elem = programme.find('title')
        raw_title = title_elem.text if title_elem is not None else ""
        title = clean_text_and_url(transform2_zh_hans(raw_title))
        if not title:
            title = "精彩节目"
        title_lang = title_elem.get('lang', 'zh') if title_elem is not None else 'zh'

        # 提取并清理 Desc
        desc_elem = programme.find('desc')
        raw_desc = desc_elem.text if desc_elem is not None else ""
        desc = clean_text_and_url(transform2_zh_hans(raw_desc))
        desc_lang = desc_elem.get('lang', 'zh') if desc_elem is not None else 'zh'

        prog_data = {
            "start": start_dt.strftime("%Y%m%d%H%M%S %z"),
            "stop": stop_dt.strftime("%Y%m%d%H%M%S %z"),
            "title": title,
            "title_lang": title_lang,
            "desc": desc,
            "desc_lang": desc_lang
        }

        # 计算得分（根据当天的 title 总长度）
        programmes[channel_id][date_str]["score"] += len(title)
        programmes[channel_id][date_str]["progs"].append(prog_data)

    # 过滤出包含今天节目的有效频道
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def write_to_xml_and_log(channels_map, programmes_map, output_dir='output'):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    log_entries = []

    for channel_id, display_names in channels_map.items():
        # 如果该频道没有节目，则跳过
        if channel_id not in programmes_map or not programmes_map[channel_id]:
            continue

        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        
        primary_name = channel_id
        for display_name, langattr in display_names:
            primary_name = display_name
            ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr}).text = display_name

        # 写入该频道的所有节目，并记录日志
        for date_str, daily_data in sorted(programmes_map[channel_id].items()):
            log_entries.append(f"频道: [{primary_name}] | 日期: {date_str} | 来源: {daily_data['source']}")
            for prog in daily_data['progs']:
                prog_elem = ET.SubElement(root, 'programme', attrib={
                    "start": prog["start"], 
                    "stop": prog["stop"], 
                    "channel": channel_id
                })
                ET.SubElement(prog_elem, 'title', attrib={"lang": prog["title_lang"]}).text = prog["title"]
                if prog["desc"]:
                    ET.SubElement(prog_elem, 'desc', attrib={"lang": prog["desc_lang"]}).text = prog["desc"]

    xml_path = os.path.join(output_dir, 'epg.xml')
    gz_path = os.path.join(output_dir, 'epg.gz')
    log_path = 'epg_source.log'

    # 输出漂亮的 XML，Python 3.9+ 专有快速方法，速度秒杀 minidom
    if hasattr(ET, 'indent'):
        ET.indent(root, space='\t', level=0)
    tree = ET.ElementTree(root)
    tree.write(xml_path, encoding='utf-8', xml_declaration=True)

    # 生成gz压缩包
    with open(xml_path, 'rb') as f_in:
        with gzip.open(gz_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # 生成日志文件
    with open(log_path, 'w', encoding='utf-8') as f_log:
        f_log.write("\n".join(log_entries))


def get_urls():
    urls = []
    if os.path.exists('config.txt'):
        with open('config.txt', 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
    return urls

async def main():
    urls = get_urls()
    if not urls:
        print("未找到配置的URL，请检查 config.txt")
        return

    exact_aliases, re_aliases = load_aliases('alias.txt')

    print("Fetching EPG data...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    # 全局合并容器
    all_channels_map = defaultdict(set)
    # merged_programmes[channel_id][date_str] = {"score": x, "source": url, "progs": []}
    merged_programmes = defaultdict(dict)

    print("Parsing and Merging EPG data...")
    for epg_content, source_url in tqdm(results, desc="Processing Sources", unit="file"):
        if not epg_content:
            continue
        
        channels, programmes = parse_epg(epg_content, source_url, exact_aliases, re_aliases)

        # 合并频道名称集合
        for cid, names in channels.items():
            all_channels_map[cid].update(names)

        # 按天合并节目，保留每日 title_score 最高的来源
        for cid, daily_progs in programmes.items():
            for date_str, daily_data in daily_progs.items():
                if date_str not in merged_programmes[cid] or daily_data["score"] > merged_programmes[cid][date_str]["score"]:
                    merged_programmes[cid][date_str] = daily_data

    print("Writing to XML and generating logs...")
    write_to_xml_and_log(all_channels_map, merged_programmes)
    print("Done! Files saved in 'output' folder. Log saved as 'epg_source.log'.")

if __name__ == '__main__':
    asyncio.run(main())
