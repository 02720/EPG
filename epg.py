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

# 1. 全局 OpenCC，避免重复创建
OPENCC_T2S = OpenCC("t2s")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
LOG_FILE = os.path.join(SCRIPT_DIR, 'epg_source.log')

URL_PATTERN = re.compile(
    r'(?ix)'
    r'(https?://[^\s<>"\']+)'
    r'|'
    r'(www\.[^\s<>"\']+)'
    r'|'
    r'(?<![@\w])(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>"\']*)?'
)

GENERIC_SITE_WORDS = {
    "官网", "官方网站", "网站", "主页", "首页",
    "home", "homepage", "officialwebsite"
}


def transform2_zh_hans(string):
    if not string:
        return ""
    return OPENCC_T2S.convert(string)


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
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None


def process_display_name(display_name):
    # 7. 去除“高清”，但保留“超高清”
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name


def strip_urls_from_text(text):
    if not text:
        return ""

    original = text
    text = URL_PATTERN.sub('', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s\-—|:：/\\,，。；;]+|[\s\-—|:：/\\,，。；;]+$', '', text).strip()

    if original != text:
        probe = re.sub(r'[\s\-—|:：/\\,，。；;]+', '', text).lower()
        if probe in GENERIC_SITE_WORDS:
            return ""

    return text


def parse_xmltv_datetime(time_str):
    """
    3. 兼容：
    - 空字符串
    - YYYYMMDDHHMMSS
    - YYYYMMDDHHMMSS+0800
    - YYYYMMDDHHMMSS +0800
    - YYYYMMDDHHMM
    - YYYYMMDD
    """
    if not time_str:
        return None

    cleaned = re.sub(r'\s+', '', time_str)
    if not cleaned:
        return None

    match = re.match(r'^(\d{8}|\d{12}|\d{14})([+-]\d{4}|Z)?$', cleaned)
    if not match:
        return None

    dt_part, tz_part = match.groups()

    if len(dt_part) == 8:
        fmt = "%Y%m%d"
    elif len(dt_part) == 12:
        fmt = "%Y%m%d%H%M"
    else:
        fmt = "%Y%m%d%H%M%S"

    try:
        dt = datetime.strptime(dt_part, fmt)
    except ValueError:
        return None

    if tz_part:
        if tz_part == 'Z':
            tzinfo = timezone.utc
        else:
            sign = 1 if tz_part[0] == '+' else -1
            hours = int(tz_part[1:3])
            minutes = int(tz_part[3:5])
            tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))
        dt = dt.replace(tzinfo=tzinfo)
    else:
        # 没有时区时，按东八区处理
        dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)

    return dt.astimezone(TZ_UTC_PLUS_8)


def deduplicate_display_names(display_names):
    result = []
    seen = set()

    for item in display_names:
        if not item or len(item) < 2:
            continue
        name, lang = item[0], item[1]
        name = (name or "").strip()
        lang = lang or "zh"
        if not name:
            continue
        key = (name, lang)
        if key not in seen:
            seen.add(key)
            result.append([name, lang])

    return result


def normalize_name_for_match(name):
    if not name:
        return ""
    name = transform2_zh_hans(name).strip()
    name = process_display_name(name)
    name = strip_urls_from_text(name).strip()
    name = name.replace('＋', '+').replace('﹢', '+')
    name = name.replace('－', '-').replace('—', '-')
    name = re.sub(r'[\s\-_]+', '', name)
    return name.upper()


def choose_primary_name(channel_id, display_names):
    for name, _ in display_names:
        if name and not name.isdigit():
            return name

    if channel_id and not channel_id.isdigit():
        return channel_id

    for name, _ in display_names:
        if name:
            return name

    return channel_id or ""


def load_alias_rules(filename='alias.txt'):
    exact_alias_map = {}
    regex_rules = []

    if not os.path.exists(filename):
        return exact_alias_map, regex_rules

    with open(filename, 'r', encoding='utf-8') as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith('#'):
                continue

            parts = [p.strip() for p in line.split(',') if p.strip()]
            if not parts:
                continue

            main_name = transform2_zh_hans(parts[0]).strip()
            main_name = process_display_name(main_name)
            if not main_name:
                continue

            main_norm = normalize_name_for_match(main_name)
            if main_norm and main_norm not in exact_alias_map:
                exact_alias_map[main_norm] = main_name

            for alias in parts[1:]:
                if alias.lower().startswith('re:'):
                    pattern = alias[3:]
                    try:
                        regex_rules.append((re.compile(pattern), main_name))
                    except re.error as e:
                        print(f"alias.txt 正则编译失败: {pattern} | 错误: {e}")
                else:
                    alias_name = transform2_zh_hans(alias).strip()
                    alias_name = process_display_name(alias_name)
                    alias_norm = normalize_name_for_match(alias_name)
                    if alias_norm and alias_norm not in exact_alias_map:
                        exact_alias_map[alias_norm] = main_name

    return exact_alias_map, regex_rules


def match_alias_main_name(candidates, exact_alias_map, regex_rules):
    for candidate in candidates:
        norm = normalize_name_for_match(candidate)
        if norm in exact_alias_map:
            return exact_alias_map[norm]

    for candidate in candidates:
        candidate_text = transform2_zh_hans(candidate).strip()
        candidate_text = process_display_name(candidate_text)
        for regex, main_name in regex_rules:
            if regex.search(candidate_text):
                return main_name

    return None


def resolve_channel_identity(channel_id, display_names, exact_alias_map, regex_rules):
    candidates = []
    if channel_id:
        candidates.append(channel_id)
    for name, _ in display_names:
        if name:
            candidates.append(name)

    alias_main = match_alias_main_name(candidates, exact_alias_map, regex_rules)
    if alias_main:
        canonical_name = alias_main
        merge_key = f"alias:{normalize_name_for_match(alias_main)}"
    else:
        canonical_name = choose_primary_name(channel_id, display_names)
        merge_key = f"norm:{normalize_name_for_match(canonical_name)}"

    resolved_display_names = [[canonical_name, 'zh']]
    resolved_display_names.extend(display_names)
    if channel_id and not channel_id.isdigit():
        resolved_display_names.append([channel_id, 'zh'])

    resolved_display_names = deduplicate_display_names(resolved_display_names)

    return merge_key, canonical_name, resolved_display_names


def build_programme_signature(prog):
    titles = []
    for title in prog.findall('title'):
        titles.append((title.get('lang', ''), (title.text or '').strip()))
    return (
        prog.get('start', ''),
        prog.get('stop', ''),
        tuple(titles)
    )


def sort_programmes(programmes):
    return sorted(
        programmes,
        key=lambda p: (
            p.get('start', ''),
            p.get('stop', ''),
            ''.join((t.text or '') for t in p.findall('title'))
        )
    )


def deduplicate_programmes(programmes):
    result = []
    seen = set()
    for prog in sort_programmes(programmes):
        sig = build_programme_signature(prog)
        if sig not in seen:
            seen.add(sig)
            result.append(prog)
    return result


def group_programmes_by_day(programmes):
    grouped = defaultdict(list)
    for prog in programmes:
        start = prog.get('start', '')
        if len(start) >= 8:
            day_key = start[:8]
            grouped[day_key].append(prog)

    for day_key in grouped:
        grouped[day_key] = sort_programmes(grouped[day_key])

    return grouped


def get_day_title_total_length(programmes):
    total = 0
    for prog in programmes:
        for title in prog.findall('title'):
            if title.text:
                total += len(title.text.strip())
    return total


def merge_display_names(existing, incoming):
    return deduplicate_display_names((existing or []) + (incoming or []))


def format_day_key(day_key):
    if len(day_key) == 8:
        return f"{day_key[:4]}-{day_key[4:6]}-{day_key[6:]}"
    return day_key


def write_source_log(source_log):
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        for channel_name in sorted(source_log.keys()):
            for day_key in sorted(source_log[channel_name].keys()):
                sources = sorted(source_log[channel_name][day_key])
                source_text = ', '.join(sources)
                f.write(f"频道: [{channel_name}] | 日期: {format_day_key(day_key)}| 来源: {source_text}\n")


def parse_epg(epg_content):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, {}

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        raw_channel_id = (channel.get('id') or '').strip()
        channel_id = transform2_zh_hans(raw_channel_id).strip()
        channel_id = process_display_name(channel_id)

        channel_display_names = []
        for name in channel.findall('display-name'):
            raw_name = (name.text or '').strip()
            if not raw_name:
                continue

            t_name = transform2_zh_hans(raw_name).strip()
            t_name = process_display_name(t_name)
            t_name = strip_urls_from_text(t_name).strip()
            if not t_name:
                continue

            channel_display_names.append([t_name, name.get('lang', 'zh') or 'zh'])

        if not channel_id and channel_display_names:
            channel_id = channel_display_names[0][0]

        if channel_id and not channel_id.isdigit() and all(node[0] != channel_id for node in channel_display_names):
            channel_display_names.append([channel_id, 'zh'])

        channel_display_names = deduplicate_display_names(channel_display_names)

        if channel_id:
            channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        raw_prog_channel = (programme.get('channel') or '').strip()
        channel_id = transform2_zh_hans(raw_prog_channel).strip()
        channel_id = process_display_name(channel_id)
        if not channel_id:
            continue

        channel_start = parse_xmltv_datetime(programme.get('start'))
        channel_stop = parse_xmltv_datetime(programme.get('stop'))

        if channel_start is None or channel_stop is None:
            continue

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        channel_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
            }
        )

        title_nodes = programme.findall('title')
        added_titles = 0
        has_nonempty_raw_title = False

        for title in title_nodes:
            raw_title = (title.text or '').strip()
            langattr = title.get('lang')

            if raw_title:
                has_nonempty_raw_title = True
                channel_title = raw_title
                if langattr == 'zh' or langattr is None:
                    channel_title = transform2_zh_hans(channel_title)

                # 2. 屏蔽节目单中的网址
                channel_title = strip_urls_from_text(channel_title).strip()
                if not channel_title:
                    continue

                channel_elem_t = ET.SubElement(channel_elem, 'title')
                channel_elem_t.text = channel_title
                if langattr is not None:
                    channel_elem_t.set('lang', langattr)
                added_titles += 1

        # 4. 仅保留 title，忽略 desc
        if added_titles == 0:
            if not title_nodes or not has_nonempty_raw_title:
                channel_elem_t = ET.SubElement(channel_elem, 'title')
                channel_elem_t.text = "精彩节目"
            else:
                # 标题本身只有网址等无效内容，直接跳过该节目
                continue

        programmes[channel_id].append(channel_elem)

    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')

    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    for channel_id in sorted(channels_id):
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        display_names = channels_names.get(channel_id, [[channel_id, 'zh']])

        for display_name_node in display_names:
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
    """
    8. config.txt:
    - [WHITELIST] 以上：非白名单
    - [WHITELIST] 以下：白名单
    - 忽略 # 开头行
    """
    urls = []
    in_whitelist = False

    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            if line.upper() == '[WHITELIST]':
                in_whitelist = True
                continue

            urls.append({
                'url': line,
                'is_whitelist': in_whitelist
            })

    return urls


async def main():
    sources = get_urls()
    exact_alias_map, regex_rules = load_alias_rules()

    tasks = [fetch_epg(item['url']) for item in sources]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished.")

    # 非白名单：按天比较 title 总长度后保留
    normal_merged = {}
    # 白名单：全部保留（做去重，不做优劣比较）
    whitelist_merged = {}

    i = 0
    for source_info, epg_content in zip(sources, epg_contents):
        i += 1
        url = source_info['url']
        is_whitelist = source_info['is_whitelist']

        source_type = "WHITELIST" if is_whitelist else "NORMAL"
        print(f"Processing EPG source...{i}/{len(epg_contents)} [{source_type}]")

        if epg_content is None:
            continue

        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        print("Finished.")

        with tqdm(total=len(channels), desc="Merging EPG", unit="channel") as pbar:
            for channel_id, display_names in channels.items():
                channel_programmes = programmes.get(channel_id, [])
                if len(channel_programmes) == 0:
                    pbar.update(1)
                    continue

                merge_key, canonical_name, resolved_display_names = resolve_channel_identity(
                    channel_id, display_names, exact_alias_map, regex_rules
                )

                if not canonical_name or not merge_key:
                    pbar.update(1)
                    continue

                day_programmes_map = group_programmes_by_day(channel_programmes)
                target = whitelist_merged if is_whitelist else normal_merged

                if merge_key not in target:
                    target[merge_key] = {
                        'channel_id': canonical_name,
                        'display_names': resolved_display_names,
                        'days': {}
                    }
                else:
                    target[merge_key]['display_names'] = merge_display_names(
                        target[merge_key]['display_names'],
                        resolved_display_names
                    )

                for day_key, day_programmes in day_programmes_map.items():
                    if is_whitelist:
                        day_entry = target[merge_key]['days'].setdefault(
                            day_key,
                            {
                                'programmes': [],
                                'sources': set()
                            }
                        )
                        day_entry['programmes'] = deduplicate_programmes(
                            day_entry['programmes'] + day_programmes
                        )
                        day_entry['sources'].add(url)
                    else:
                        score = get_day_title_total_length(day_programmes)
                        existing = target[merge_key]['days'].get(day_key)

                        if existing is None or score > existing['score']:
                            target[merge_key]['days'][day_key] = {
                                'programmes': day_programmes,
                                'score': score,
                                'sources': {url}
                            }

                pbar.update(1)

    # 汇总最终结果
    final_channel_names = defaultdict(list)
    final_programmes = defaultdict(list)
    source_log = defaultdict(lambda: defaultdict(set))

    whitelist_merge_keys = set(whitelist_merged.keys())
    whitelist_channel_ids = {v['channel_id'] for v in whitelist_merged.values()}

    # 先写入白名单
    for merge_key, entry in whitelist_merged.items():
        final_channel_names[entry['channel_id']] = merge_display_names(
            final_channel_names[entry['channel_id']],
            [[entry['channel_id'], 'zh']] + entry['display_names']
        )

        for day_key, day_entry in entry['days'].items():
            final_programmes[entry['channel_id']].extend(day_entry['programmes'])
            source_log[entry['channel_id']][day_key].update(day_entry['sources'])

    # 再写入非白名单；若白名单存在相同频道，则忽略非白名单
    for merge_key, entry in normal_merged.items():
        if merge_key in whitelist_merge_keys or entry['channel_id'] in whitelist_channel_ids:
            continue

        final_channel_names[entry['channel_id']] = merge_display_names(
            final_channel_names[entry['channel_id']],
            [[entry['channel_id'], 'zh']] + entry['display_names']
        )

        for day_key, day_entry in entry['days'].items():
            final_programmes[entry['channel_id']].extend(day_entry['programmes'])
            source_log[entry['channel_id']][day_key].update(day_entry['sources'])

    # 最终去重排序
    for channel_id in list(final_programmes.keys()):
        final_programmes[channel_id] = deduplicate_programmes(final_programmes[channel_id])
        final_programmes[channel_id] = sort_programmes(final_programmes[channel_id])

    # 写日志
    write_source_log(source_log)

    print("Writing to XML...")
    write_to_xml(
        list(final_programmes.keys()),
        final_channel_names,
        final_programmes,
        'output/epg.xml'
    )
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print(f"EPG source log written to: {LOG_FILE}")


if __name__ == '__main__':
    asyncio.run(main())
