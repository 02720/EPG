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

try:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    BASE_DIR = os.getcwd()

CONFIG_FILE = os.path.join(BASE_DIR, 'config.txt')
ALIAS_FILE = os.path.join(BASE_DIR, 'alias.txt')
LOG_FILE = os.path.join(BASE_DIR, 'epg_source.log')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

# 1. 全局 OpenCC，避免重复创建
CC_T2S = OpenCC("t2s")

# 用于屏蔽节目单中的网址
URL_PATTERN = re.compile(
    r'(?i)\b(?:https?://|www\.)[^\s<>"\']+|(?<!@)\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>"\']*)?'
)


def transform2_zh_hans(string):
    if string is None:
        return ""
    return CC_T2S.convert(string)


def strip_urls_from_text(text):
    if text is None:
        return ""
    text = URL_PATTERN.sub('', text)
    text = re.sub(r'\s+', ' ', text)
    text = text.strip(" \t\r\n-|｜_/，,;；")
    return text.strip()


def process_display_name(display_name):
    display_name = (display_name or "").strip()
    # 7. 去除“高清”，但不去除“超高清”
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name.strip()


def basic_clean_channel_name(name):
    name = transform2_zh_hans(name or "")
    name = process_display_name(name)
    return name.strip()


def normalize_channel_name(name):
    name = basic_clean_channel_name(name)
    name = name.replace('　', ' ')
    name = re.sub(r'[\s\-_]+', '', name)
    return name.upper()


def is_safe_merge_name(name):
    """
    额外增加的防串台机制：
    只对“相对安全”的频道名做跨源匹配，避免像“综合”“电影”这类过于泛化的名称误合并。
    """
    if not name:
        return False

    normalized = normalize_channel_name(name)
    if not normalized:
        return False

    # 含字母/数字的频道名通常更安全，例如 CCTV1、CGTN、4K 等
    if re.search(r'[A-Z0-9]', normalized):
        return True

    compact = re.sub(r'\s+', '', name)

    # 长度较长的中文频道名，一般相对安全
    if len(compact) >= 4:
        return True

    # 一些常见频道后缀，也视为相对安全
    if re.search(r'(台|频道|卫视|影视|电视|卡通|新闻|纪实|少儿|影院|剧场|音乐|体育|中文|国际|电影|教育|财经|生活|综艺|农业|科教|戏曲|军事|法治|电竞)$', compact):
        return True

    return False


def load_alias_rules():
    exact_alias = {}
    regex_alias = []

    if not os.path.exists(ALIAS_FILE):
        return {
            'exact': exact_alias,
            'regex': regex_alias
        }

    with open(ALIAS_FILE, 'r', encoding='utf-8') as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue

            parts = [part.strip() for part in line.split(',') if part.strip()]
            if not parts:
                continue

            master = basic_clean_channel_name(parts[0])
            if not master:
                continue

            exact_alias[normalize_channel_name(master)] = master

            for alias in parts[1:]:
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        regex_alias.append((re.compile(pattern), master))
                    except re.error as e:
                        print(f"alias.txt 正则错误：{alias} -> {e}")
                else:
                    alias_name = basic_clean_channel_name(alias)
                    if alias_name:
                        exact_alias[normalize_channel_name(alias_name)] = master

    return {
        'exact': exact_alias,
        'regex': regex_alias
    }


def match_alias(name, alias_rules):
    if not name:
        return None

    norm_name = normalize_channel_name(name)

    if norm_name in alias_rules['exact']:
        return alias_rules['exact'][norm_name]

    for pattern, master in alias_rules['regex']:
        if pattern.search(name):
            return master

    return None


def standardize_channel_name(name, alias_rules):
    cleaned_name = basic_clean_channel_name(name)
    if not cleaned_name:
        return "", False

    master = match_alias(cleaned_name, alias_rules)
    if master:
        return master, True

    return cleaned_name, False


def reorder_display_names(display_names, preferred_name):
    ordered = []
    seen = set()

    if preferred_name:
        ordered.append([preferred_name, 'zh'])
        seen.add(preferred_name)

    for name, lang in display_names:
        if name and name not in seen:
            ordered.append([name, lang or 'zh'])
            seen.add(name)

    return ordered


def standardize_channel_names(channel_id, display_names, alias_rules):
    standardized = []
    seen = set()
    alias_preferred = None

    candidates = list(display_names)
    if channel_id and not str(channel_id).isdigit():
        candidates.append([channel_id, 'zh'])

    for name, lang in candidates:
        std_name, matched_alias = standardize_channel_name(name, alias_rules)
        if not std_name:
            continue

        if matched_alias and alias_preferred is None:
            alias_preferred = std_name

        if std_name not in seen:
            standardized.append([std_name, lang or 'zh'])
            seen.add(std_name)

    preferred_name = alias_preferred

    if preferred_name is None:
        for name, _ in standardized:
            if name and not str(name).isdigit() and is_safe_merge_name(name):
                preferred_name = name
                break

    if preferred_name is None:
        for name, _ in standardized:
            if name and not str(name).isdigit():
                preferred_name = name
                break

    if preferred_name is None:
        std_channel_id, _ = standardize_channel_name(channel_id, alias_rules)
        preferred_name = std_channel_id or channel_id

    standardized = reorder_display_names(standardized, preferred_name)
    return standardized, preferred_name


def iter_merge_candidate_names(preferred_name, display_names):
    seen = set()

    if preferred_name and not str(preferred_name).isdigit() and is_safe_merge_name(preferred_name):
        yield preferred_name
        seen.add(preferred_name)

    for name, _ in display_names:
        if not name or name in seen:
            continue
        if str(name).isdigit():
            continue
        if is_safe_merge_name(name):
            yield name
            seen.add(name)


def find_existing_map_id(preferred_name, display_names, all_channels_map):
    matched_ids = []

    for name in iter_merge_candidate_names(preferred_name, display_names):
        key = normalize_channel_name(name)
        if key in all_channels_map:
            matched_ids.append(all_channels_map[key])

    matched_ids = list(dict.fromkeys(matched_ids))

    if len(matched_ids) == 1:
        return matched_ids[0]

    # 多个不同结果时，为避免串台，不强行合并
    return None


def register_channel_map_keys(map_id, display_names, all_channels_map):
    for name in iter_merge_candidate_names(map_id, display_names):
        key = normalize_channel_name(name)
        if key:
            all_channels_map[key] = map_id


def merge_display_name_list(existing_names, new_names):
    seen = {name for name, _ in existing_names}
    for name, lang in new_names:
        if name and name not in seen:
            existing_names.append([name, lang])
            seen.add(name)


def parse_programme_time(value):
    """
    修复：
    1. 空字符串报错
    2. 无时区字符串报错
    """
    if value is None:
        return None

    value = value.strip()
    if not value:
        return None

    value = re.sub(r'\s+', ' ', value)

    timezone_formats = [
        "%Y%m%d%H%M%S %z",
        "%Y%m%d%H%M%S%z",
        "%Y%m%d%H%M %z",
        "%Y%m%d%H%M%z",
    ]
    for fmt in timezone_formats:
        try:
            return datetime.strptime(value, fmt).astimezone(TZ_UTC_PLUS_8)
        except ValueError:
            pass

    compact_value = value.replace(' ', '')
    naive_formats = [
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
    ]
    for fmt in naive_formats:
        try:
            dt = datetime.strptime(compact_value, fmt)
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
        except ValueError:
            pass

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
        channel_id = basic_clean_channel_name(channel.get('id'))
        if not channel_id:
            continue

        channel_display_names = []
        seen_names = set()

        for name in channel.findall('display-name'):
            if name.text is None:
                continue

            t_name = basic_clean_channel_name(name.text)
            if not t_name:
                continue

            if t_name not in seen_names:
                channel_display_names.append([t_name, name.get('lang', 'zh')])
                seen_names.add(t_name)

        if not channel_id.isdigit() and channel_id not in seen_names:
            channel_display_names.append([channel_id, 'zh'])

        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        channel_id = basic_clean_channel_name(programme.get('channel'))
        if not channel_id:
            continue

        channel_start = parse_programme_time(programme.get('start'))
        channel_stop = parse_programme_time(programme.get('stop'))

        if channel_start is None or channel_stop is None:
            continue

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        prepared_titles = []
        had_url_only_title = False

        for title in programme.findall('title'):
            langattr = title.get('lang')
            raw_title = (title.text or '').strip()

            if not raw_title:
                continue

            channel_title = raw_title
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)

            # 2. 屏蔽节目单中的网址
            channel_title = strip_urls_from_text(channel_title)

            if not channel_title:
                had_url_only_title = True
                continue

            prepared_titles.append((channel_title, langattr))

        # 若标题全是网址，则直接跳过该节目，避免生成“精彩节目”假数据
        if not prepared_titles:
            if had_url_only_title:
                continue
            prepared_titles.append(("精彩节目", None))

        channel_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
            }
        )

        for channel_title, langattr in prepared_titles:
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)

        # 4. 仅保留 title，忽略 desc
        programmes[channel_id].append(channel_elem)

    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def group_programmes_by_day(programme_list):
    grouped = defaultdict(list)
    for prog in programme_list:
        start_dt = parse_programme_time(prog.get('start'))
        if start_dt is None:
            continue
        grouped[start_dt.date()].append(prog)
    return grouped


def calculate_day_title_length(programme_list):
    total_length = 0
    for prog in programme_list:
        for title in prog.findall('title'):
            if title.text:
                total_length += len(title.text.strip())
    return total_length


def get_channel_log_name(display_names, fallback):
    for name, _ in display_names:
        if name and not str(name).isdigit():
            return name
    if display_names:
        return display_names[0][0]
    return fallback


def write_epg_source_log(channel_names, programme_sources, final_channel_ids):
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        for channel_id in sorted(final_channel_ids):
            display_names = channel_names.get(channel_id, [])
            channel_name = get_channel_log_name(display_names, channel_id)

            day_source_map = programme_sources.get(channel_id, {})
            for day in sorted(day_source_map.keys()):
                source = day_source_map[day]
                f.write(
                    f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')} | 来源: {source}\n"
                )


def write_to_xml(channels_id, channels_names, programmes, filename):
    output_dir = os.path.dirname(filename)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    for channel_id in sorted(channels_id):
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name

        for prog in programmes.get(channel_id, []):
            prog.set('channel', channel_id)
            root.append(prog)

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
    with open(CONFIG_FILE, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


async def main():
    alias_rules = load_alias_rules()
    urls = get_urls()
    tasks = [fetch_epg(url) for url in urls]

    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished.")

    all_channels_map = {}
    all_channel_names = defaultdict(list)

    # 按频道 + 日期存储
    all_programmes_by_day = defaultdict(dict)
    all_programme_scores = defaultdict(dict)
    all_programme_sources = defaultdict(dict)

    for i, (url, epg_content) in enumerate(zip(urls, epg_contents), start=1):
        print(f"Processing EPG source...{i}/{len(epg_contents)}")
        if epg_content is None:
            continue

        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        print("Finished.")

        with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
            for channel_id, display_names in channels.items():
                channel_programmes = programmes.get(channel_id, [])
                if len(channel_programmes) == 0:
                    pbar.update(1)
                    continue

                # 6. 别名先转换为主名，再统一合成
                standardized_names, preferred_name = standardize_channel_names(
                    channel_id, display_names, alias_rules
                )

                map_id = find_existing_map_id(preferred_name, standardized_names, all_channels_map)
                if map_id is None:
                    map_id = preferred_name or channel_id

                ordered_names = reorder_display_names(standardized_names, map_id)

                if not all_channel_names[map_id]:
                    all_channel_names[map_id] = ordered_names
                else:
                    merge_display_name_list(all_channel_names[map_id], ordered_names)

                register_channel_map_keys(map_id, all_channel_names[map_id], all_channels_map)

                # 4. 以“同一频道 + 同一天”的 title 总长度来决定保留哪个来源
                grouped_programmes = group_programmes_by_day(channel_programmes)
                for day, day_programmes in grouped_programmes.items():
                    score = calculate_day_title_length(day_programmes)

                    if day not in all_programme_scores[map_id] or score > all_programme_scores[map_id][day]:
                        all_programme_scores[map_id][day] = score
                        all_programmes_by_day[map_id][day] = day_programmes
                        all_programme_sources[map_id][day] = url

                pbar.update(1)

    all_programmes = {}
    for channel_id, day_map in all_programmes_by_day.items():
        merged_programmes = []
        for day in sorted(day_map.keys()):
            day_programmes = sorted(day_map[day], key=lambda x: x.get('start', ''))
            merged_programmes.extend(day_programmes)

        if merged_programmes:
            all_programmes[channel_id] = merged_programmes

    final_channel_ids = [channel_id for channel_id in all_channel_names.keys() if channel_id in all_programmes]

    print("Writing source log...")
    write_epg_source_log(all_channel_names, all_programme_sources, final_channel_ids)

    print("Writing to XML...")
    epg_xml_path = os.path.join(OUTPUT_DIR, 'epg.xml')
    epg_gz_path = os.path.join(OUTPUT_DIR, 'epg.gz')

    write_to_xml(final_channel_ids, all_channel_names, all_programmes, epg_xml_path)
    compress_to_gz(epg_xml_path, epg_gz_path)


if __name__ == '__main__':
    asyncio.run(main())
