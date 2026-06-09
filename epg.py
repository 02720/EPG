import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio  # 引入 tqdm 的异步支持
from datetime import datetime, timezone, timedelta
import gzip
import shutil
from xml.dom import minidom
import re
from opencc import OpenCC
import os
from tqdm import tqdm  # 引入 tqdm 的同步支持

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 全局 OpenCC，避免重复创建
CC_T2S = OpenCC("t2s")


def transform2_zh_hans(string):
    if string is None:
        return ""
    new_str = CC_T2S.convert(string)
    return new_str


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
                    return gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    return await response.text(encoding='utf-8')
    except aiohttp.ClientError as e:
        print(f"{url}HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url}请求超时")
    except Exception as e:
        print(f"{url}其他错误: {e}")
    return None


def process_display_name(display_name):
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name


def parse_xmltv_datetime(time_str):
    if time_str is None:
        return None

    time_str = re.sub(r'\s+', '', time_str.strip())
    if not time_str:
        return None

    if time_str.endswith('Z'):
        time_str = time_str[:-1] + '+0000'

    try:
        if re.fullmatch(r'\d{14}', time_str):
            return datetime.strptime(time_str, "%Y%m%d%H%M%S").replace(tzinfo=TZ_UTC_PLUS_8)
        return datetime.strptime(time_str, "%Y%m%d%H%M%S%z")
    except ValueError:
        return None


def is_homepage_programme(programme):
    texts = []
    for tag in ('title', 'desc'):
        for elem in programme.findall(tag):
            if elem.text:
                texts.append(elem.text.strip())

    text = " ".join(texts)

    if not text:
        return False

    if re.search(r'由\s*https?://[^\s<>"，,。]+?\s*提供服务', text):
        return True

    if 'https://epg.136605.xyz' in text or 'http://epg.136605.xyz' in text:
        return True

    for item in texts:
        if re.fullmatch(r'https?://[^\s<>"，,。]+/?', item):
            return True

    return False


def parse_epg(epg_content):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if not channel_id.isdigit() and not any(channel_id == item[0] for item in channel_display_names):
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        if is_homepage_programme(programme):
            continue

        channel_id = transform2_zh_hans(programme.get('channel'))

        channel_start = parse_xmltv_datetime(programme.get('start'))
        channel_stop = parse_xmltv_datetime(programme.get('stop'))

        if channel_start is None or channel_stop is None:
            continue

        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        channel_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
            }
        )

        for title in programme.findall('title'):
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)

        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            langattr = desc.get('lang')
            channel_desc = desc.text.strip()
            if langattr == 'zh' or langattr is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(channel_elem, 'desc')
            channel_elem_d.text = channel_desc.strip()
            if langattr is not None:
                channel_elem_d.set('lang', langattr)

        programmes[channel_id].append(channel_elem)

    # Filter channels that don't have any program ending today
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def write_to_xml(channels_id, channels_names, programmes, filename):
    # 目录不存在
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    for channel_id in channels_id:
        channel_elem = ET.SubElement(
            root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(
                channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
        for prog in programmes[channel_id]:
            prog.set('channel', channel_id)  # 设置 programme 的 channel 属性
            root.append(prog)

    # Beautify the XML output
    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    urls = []
    is_whitelist = False
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.upper() == '[WHITELIST]':
                is_whitelist = True
                continue
            urls.append((line, is_whitelist))
    return urls


def load_aliases():
    alias_exact_map = {}
    alias_regex_list = []
    alias_output_map = defaultdict(list)

    alias_file = 'alias.txt'
    if not os.path.exists(alias_file):
        return alias_exact_map, alias_regex_list, alias_output_map

    with open(alias_file, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = [item.strip() for item in line.split(',') if item.strip()]
            if not parts:
                continue

            master = process_display_name(transform2_zh_hans(parts[0]))
            if not master:
                continue

            if master not in alias_output_map[master]:
                alias_output_map[master].append(master)

            alias_exact_map[master] = master

            for alias in parts[1:]:
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        alias_regex_list.append((master, re.compile(pattern)))
                    except re.error as e:
                        print(f"alias.txt 正则表达式错误: {alias}，错误: {e}")
                    continue

                alias_name = process_display_name(transform2_zh_hans(alias))
                if not alias_name:
                    continue

                alias_exact_map[alias_name] = master
                if alias_name not in alias_output_map[master]:
                    alias_output_map[master].append(alias_name)

    return alias_exact_map, alias_regex_list, alias_output_map


def resolve_alias(channel_id, display_names, alias_exact_map, alias_regex_list):
    names = [channel_id] + [item[0] for item in display_names]

    for name in names:
        if name in alias_exact_map:
            return alias_exact_map[name]

    for name in names:
        for master, pattern in alias_regex_list:
            if pattern.search(name):
                return master

    return None


def get_alias_display_names(master, alias_output_map):
    if master in alias_output_map:
        return [[name, 'zh'] for name in alias_output_map[master]]
    return [[master, 'zh']]


def get_programme_day(programme):
    start_time = parse_xmltv_datetime(programme.get('start'))
    if start_time is None:
        return None
    return start_time.astimezone(TZ_UTC_PLUS_8).date()


def get_programme_title_length(programmes):
    total_length = 0
    for prog in programmes:
        for title in prog.findall('title'):
            if title.text:
                total_length += len(title.text.strip())
    return total_length


def group_programmes_by_day(programmes):
    grouped = defaultdict(list)
    for prog in programmes:
        day = get_programme_day(prog)
        if day is None:
            continue
        grouped[day].append(prog)
    return grouped


def sort_programmes(programmes):
    return sorted(
        programmes,
        key=lambda prog: parse_xmltv_datetime(prog.get('start')) or datetime.min.replace(tzinfo=TZ_UTC_PLUS_8)
    )


def get_primary_channel_name(channel_id, channel_names):
    if channel_names.get(channel_id):
        return channel_names[channel_id][0][0]
    return channel_id


def write_source_log(channels_id, channels_names, day_sources):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(script_dir, 'epg_source.log')

    with open(log_path, 'w', encoding='utf-8') as f:
        for channel_id in channels_id:
            channel_name = get_primary_channel_name(channel_id, channels_names)
            for day in sorted(day_sources[channel_id].keys()):
                for source in day_sources[channel_id][day]:
                    f.write(f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')}| 来源: {source}\n")


async def main():
    url_items = get_urls()
    urls = [item[0] for item in url_items]

    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    alias_exact_map, alias_regex_list, alias_output_map = load_aliases()

    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)

    all_programmes_by_day = defaultdict(lambda: defaultdict(list))
    all_programmes_title_length = defaultdict(dict)
    all_programmes_source = defaultdict(lambda: defaultdict(list))
    whitelist_channels = set()

    print("Finished.")
    i = 0
    for epg_content, url_item in zip(epg_contents, url_items):
        source_url, is_whitelist_source = url_item

        i += 1
        print(f"Processing EPG source...{i}/{len(epg_contents)}")
        if epg_content is None:
            continue

        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        print("Finished.")

        with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
            for channel_id, display_names in channels.items():
                if len(programmes[channel_id]) == 0:
                    pbar.update(1)
                    continue

                alias_master = resolve_alias(channel_id, display_names, alias_exact_map, alias_regex_list)

                if alias_master:
                    map_id = alias_master
                    display_names_for_merge = get_alias_display_names(alias_master, alias_output_map)
                    is_in_map = map_id in all_channel_id
                else:
                    is_in_map = False
                    map_id = ""
                    for display_name_node in display_names:
                        if is_in_map:
                            break
                        display_name = display_name_node[0]
                        is_in_map = display_name in all_channels_map
                        map_id = display_name
                    map_id = all_channels_map.get(map_id, channel_id)
                    display_names_for_merge = display_names

                if not is_in_map:
                    all_channel_id.add(map_id)
                    all_channel_names[map_id] = display_names_for_merge
                    for display_name_node in display_names_for_merge:
                        display_name = display_name_node[0]
                        all_channels_map[display_name] = map_id
                else:
                    for display_name_node in display_names_for_merge:
                        display_name = display_name_node[0]
                        if display_name not in all_channels_map:
                            all_channel_names[map_id].append(display_name_node)
                            all_channels_map[display_name] = map_id

                programmes_by_day = group_programmes_by_day(programmes[channel_id])

                if is_whitelist_source:
                    if map_id not in whitelist_channels:
                        all_programmes_by_day[map_id] = defaultdict(list)
                        all_programmes_title_length[map_id] = {}
                        all_programmes_source[map_id] = defaultdict(list)
                        whitelist_channels.add(map_id)

                    for day, day_programmes in programmes_by_day.items():
                        all_programmes_by_day[map_id][day].extend(day_programmes)
                        if source_url not in all_programmes_source[map_id][day]:
                            all_programmes_source[map_id][day].append(source_url)
                else:
                    if map_id in whitelist_channels:
                        pbar.update(1)
                        continue

                    for day, day_programmes in programmes_by_day.items():
                        title_length = get_programme_title_length(day_programmes)
                        old_title_length = all_programmes_title_length[map_id].get(day, -1)

                        if day not in all_programmes_by_day[map_id] or title_length > old_title_length:
                            all_programmes_by_day[map_id][day] = day_programmes
                            all_programmes_title_length[map_id][day] = title_length
                            all_programmes_source[map_id][day] = [source_url]

                pbar.update(1)  # 更新进度条

    all_programmes = defaultdict(list)
    final_channel_id = []

    for channel_id in all_channel_id:
        for day in sorted(all_programmes_by_day[channel_id].keys()):
            all_programmes[channel_id].extend(sort_programmes(all_programmes_by_day[channel_id][day]))

        if all_programmes[channel_id]:
            final_channel_id.append(channel_id)

    print("Writing to XML...")
    write_to_xml(final_channel_id, all_channel_names, all_programmes, 'output/epg.xml')
    write_source_log(final_channel_id, all_channel_names, all_programmes_source)
    compress_to_gz('output/epg.xml', 'output/epg.gz')


if __name__ == '__main__':
    asyncio.run(main())
