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

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 1. 全局OpenCC，避免重复创建
cc = OpenCC("t2s")

def transform2_zh_hans(string):
    if not string:
        return string
    return cc.convert(string)

# 6. 加载别名配置
def load_aliases():
    aliases = {}
    regex_aliases = []
    alias_to_add = defaultdict(list)
    if os.path.exists('alias.txt'):
        with open('alias.txt', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                parts = line.split(',')
                main_name = parts[0]
                for alias in parts[1:]:
                    if alias.startswith('re:'):
                        try:
                            regex_aliases.append((re.compile(alias[3:]), main_name))
                        except re.error:
                            print(f"正则表达式错误: {alias}")
                    else:
                        aliases[alias] = main_name
                        alias_to_add[main_name].append(alias)
    return aliases, regex_aliases, alias_to_add

def get_main_name(name, aliases, regex_aliases):
    if not name:
        return name
    if name in aliases:
        return aliases[name]
    for pattern, main_name in regex_aliases:
        if pattern.match(name):
            return main_name
    return name

# 9. 判断是否为纯外文或外文占比过高
def is_pure_foreign(text):
    if not text: return False
    return not bool(re.search(r'[\u4e00-\u9fa5]', text))

def is_mostly_foreign_titles(titles):
    if not titles: return False
    total_len = 0
    zh_len = 0
    for t in titles:
        clean_t = re.sub(r'\s+', '', t)
        total_len += len(clean_t)
        zh_len += len(re.findall(r'[\u4e00-\u9fa5]', clean_t))
    if total_len == 0: return False
    # 中文字符占比低于40%，即外文超过60%
    return (zh_len / total_len) < 0.4

# 3. 解决时间格式报错
def parse_time(time_str):
    if not time_str:
        return None
    time_str = re.sub(r'\s+', '', time_str)
    if len(time_str) == 14:  # 缺失时区，默认补全 +0800
        time_str += '+0800'
    try:
        return datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
    except ValueError:
        return None

async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    return url, gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return url, await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return url, None

# 7. 去除“高清”，保留“超高清”
def process_display_name(display_name):
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name

def parse_epg(epg_content, aliases, regex_aliases):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return {}, {}, {}

    channels = {}
    channel_id_to_main = {}
    
    # 解析频道并映射为主名
    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        display_names = []
        best_name = channel_id
        
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            display_names.append([t_name, name.get('lang', 'zh')])
            if t_name:
                best_name = t_name
                
        main_name = get_main_name(best_name, aliases, regex_aliases)
        channel_id_to_main[channel.get('id')] = main_name
        
        # 9. 剔除纯外文频道
        if is_pure_foreign(main_name):
            continue

        if main_name not in channels:
            channels[main_name] = []
        channels[main_name].extend(display_names)

    today = datetime.now(TZ_UTC_PLUS_8).date()
    
    # programmes 结构: dict[main_name][date_str] = list of elements
    programmes = defaultdict(lambda: defaultdict(list))
    # title_lengths 结构: dict[main_name][date_str] = int (title总长度)
    title_lengths = defaultdict(lambda: defaultdict(int))
    channel_titles_for_lang_check = defaultdict(list)

    for programme in root.findall('programme'):
        orig_channel_id = programme.get('channel')
        if orig_channel_id not in channel_id_to_main:
            continue
            
        main_name = channel_id_to_main[orig_channel_id]
        
        channel_start = parse_time(programme.get('start'))
        channel_stop = parse_time(programme.get('stop'))
        if not channel_start or not channel_stop:
            continue

        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)
        date_str = channel_start.strftime("%Y-%m-%d")

        # 提取并处理 title
        titles = programme.findall('title')
        channel_title = "精彩节目"
        if titles and titles[0].text:
            channel_title = titles[0].text.strip()

        # 2. 屏蔽广告节目单
        if "提供服务" in channel_title or re.search(r'http[s]?://', channel_title, re.I) or ".xyz" in channel_title.lower():
            continue

        langattr = titles[0].get('lang') if titles else None
        if langattr == 'zh' or langattr is None:
            channel_title = transform2_zh_hans(channel_title)
            
        channel_titles_for_lang_check[main_name].append(channel_title)

        channel_elem = ET.Element('programme', attrib={"start": channel_start.strftime("%Y%m%d%H%M%S %z"), "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")})
        
        channel_elem_t = ET.SubElement(channel_elem, 'title')
        channel_elem_t.text = channel_title
        if langattr is not None:
            channel_elem_t.set('lang', langattr)
            
        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            langattr_d = desc.get('lang')
            channel_desc = desc.text.strip()
            if langattr_d == 'zh' or langattr_d is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(channel_elem, 'desc')
            channel_elem_d.text = channel_desc.strip()
            if langattr_d is not None:
                channel_elem_d.set('lang', langattr_d)
                
        programmes[main_name][date_str].append(channel_elem)
        title_lengths[main_name][date_str] += len(channel_title)

    # 9. 剔除节目信息超过60%为外文的频道
    final_channels = {}
    final_programmes = {}
    final_lengths = {}
    
    for main_name in list(channels.keys()):
        if is_mostly_foreign_titles(channel_titles_for_lang_check[main_name]):
            continue
        final_channels[main_name] = channels[main_name]
        final_programmes[main_name] = programmes[main_name]
        final_lengths[main_name] = title_lengths[main_name]

    return final_channels, final_programmes, final_lengths

def write_to_xml_and_log(merged_programmes, all_channels_names, alias_to_add, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
        
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    log_lines = []

    for main_name, date_dict in merged_programmes.items():
        if not date_dict:
            continue
            
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": main_name})
        seen_names = set()
        
        # 写入主名
        ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"}).text = main_name
        seen_names.add(main_name)
        
        # 6. 重新映射 alias.txt 中的别名（除正则外）
        for alias in alias_to_add.get(main_name, []):
            if alias not in seen_names:
                ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"}).text = alias
                seen_names.add(alias)
                
        # 写入解析到的其他 display-name
        for d_name, lang in all_channels_names[main_name]:
            if d_name not in seen_names:
                ET.SubElement(channel_elem, 'display-name', attrib={"lang": lang}).text = d_name
                seen_names.add(d_name)

        # 写入节目并记录日志
        for date_str in sorted(date_dict.keys()):
            info = date_dict[date_str]
            # 5. 记录日志
            log_lines.append(f"频道: [{main_name}] | 日期: {date_str} | 来源: {info['url']}")
            for prog in info['elems']:
                prog.set('channel', main_name)
                root.append(prog)

    # 写入 XML
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))
        
    # 5. 写入日志文件
    with open('epg_source.log', 'w', encoding='utf-8') as f:
        f.write('\n'.join(log_lines))

def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

# 8. 增加白名单机制
def get_urls():
    normal_urls = []
    whitelist_urls = []
    is_whitelist = False
    if os.path.exists('config.txt'):
        with open('config.txt', 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if line == '[WHITELIST]':
                    is_whitelist = True
                    continue
                if is_whitelist:
                    whitelist_urls.append(line)
                else:
                    normal_urls.append(line)
    return normal_urls, whitelist_urls

async def main():
    normal_urls, whitelist_urls = get_urls()
    all_urls = normal_urls + whitelist_urls
    aliases, regex_aliases, alias_to_add = load_aliases()
    
    tasks = [fetch_epg(url) for url in all_urls]
    print("Fetching EPG data...")
    epg_results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    
    all_channels_names = defaultdict(list)
    # merged_programmes 结构: dict[main_name][date_str] = {'url': url, 'elems': [...], 'length': int, 'is_whitelist': bool}
    merged_programmes = defaultdict(lambda: defaultdict(dict))
    
    print("Finished fetching. Start parsing and merging...")
    
    for i, (url, epg_content) in enumerate(epg_results, 1):
        if epg_content is None:
            continue
            
        is_whitelist = url in whitelist_urls
        channels, programmes, lengths = parse_epg(epg_content, aliases, regex_aliases)
        
        for main_name, date_dict in programmes.items():
            all_channels_names[main_name].extend(channels.get(main_name, []))
            
            for date_str, elems in date_dict.items():
                if not elems: continue
                
                length = lengths[main_name][date_str]
                current = merged_programmes[main_name].get(date_str)
                
                should_replace = False
                # 4 & 8. 合并逻辑：白名单优先；同级别比较 title 总长度
                if not current:
                    should_replace = True
                else:
                    if is_whitelist and not current['is_whitelist']:
                        should_replace = True
                    elif not is_whitelist and current['is_whitelist']:
                        should_replace = False
                    else:
                        if length > current['length']:
                            should_replace = True
                            
                if should_replace:
                    merged_programmes[main_name][date_str] = {
                        'url': url,
                        'elems': elems,
                        'length': length,
                        'is_whitelist': is_whitelist
                    }

    print("Writing to XML and generating logs...")
    write_to_xml_and_log(merged_programmes, all_channels_names, alias_to_add, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("All tasks completed successfully.")

if __name__ == '__main__':
    asyncio.run(main())
