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

# 全局 OpenCC，避免重复创建
CC_T2S = OpenCC("t2s")

# 过滤某些 EPG 源插入的主页宣传节目
EPG_AD_PATTERNS = [
    re.compile(r'^\s*由\s*https?://[^\s]+?\s*提供服务\s*$', re.I),
    re.compile(r'^\s*https?://[^\s]+?\s*提供服务\s*$', re.I),
    re.compile(r'^\s*EPG\s*由\s*https?://[^\s]+?\s*提供服务\s*$', re.I),
]


def transform2_zh_hans(string):
    if string is None:
        return ""
    return CC_T2S.convert(string)


def process_display_name(display_name):
    # 去除“高清”，但不要去除“超高清”
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name


def should_filter_programme_title(title_text):
    if not title_text:
        return False
    title_text = title_text.strip()
    for pattern in EPG_AD_PATTERNS:
        if pattern.match(title_text):
            return True
    return False


def parse_xmltv_datetime(dt_str):
    if dt_str is None:
        return None
    dt_str = re.sub(r'\s+', ' ', dt_str.strip())
    if not dt_str:
        return None

    # 兼容：
    # 20260301000600 +0800
    # 20260301000600+0800
    # 20260301000600
    m = re.match(r'^(\d{14})(?:\s*([+-]\d{4}))?$', dt_str)
    if not m:
        return None

    time_part = m.group(1)
    tz_part = m.group(2)

    try:
        if tz_part:
            return datetime.strptime(time_part + tz_part, "%Y%m%d%H%M%S%z")
        else:
            # 无时区时按 UTC+8 处理
            return datetime.strptime(time_part, "%Y%m%d%H%M%S").replace(tzinfo=TZ_UTC_PLUS_8)
    except ValueError:
        return None


def get_programme_day_key(programme):
    start_str = programme.get("start")
    dt = parse_xmltv_datetime(start_str)
    if dt is None:
        return None
    return dt.astimezone(TZ_UTC_PLUS_8).date()


def get_programme_title_text(programme):
    titles = []
    for title in programme.findall('title'):
        if title.text:
            titles.append(title.text.strip())
    return ''.join(titles)


def calculate_daily_title_lengths(programmes):
    daily_lengths = defaultdict(int)
    for prog in programmes:
        day = get_programme_day_key(prog)
        if day is None:
            continue
        daily_lengths[day] += len(get_programme_title_text(prog))
    return daily_lengths


def merge_programmes_by_day(existing_programmes, new_programmes, prefer_new=False):
    """
    按“天”合并节目：
    - prefer_new=True: 新来源整天直接覆盖旧来源
    - prefer_new=False: 比较当天所有 title 总长度，保留更长者
    返回合并后的 programme 列表，以及每一天是否使用了 new_programmes
    """
    existing_by_day = defaultdict(list)
    new_by_day = defaultdict(list)

    for prog in existing_programmes:
        day = get_programme_day_key(prog)
        if day is not None:
            existing_by_day[day].append(prog)

    for prog in new_programmes:
        day = get_programme_day_key(prog)
        if day is not None:
            new_by_day[day].append(prog)

    all_days = set(existing_by_day.keys()) | set(new_by_day.keys())
    merged_by_day = {}
    day_source_is_new = {}

    for day in all_days:
        old_list = existing_by_day.get(day, [])
        new_list = new_by_day.get(day, [])

        if not old_list:
            merged_by_day[day] = new_list
            day_source_is_new[day] = True
            continue
        if not new_list:
            merged_by_day[day] = old_list
            day_source_is_new[day] = False
            continue

        if prefer_new:
            merged_by_day[day] = new_list
            day_source_is_new[day] = True
        else:
            old_len = sum(len(get_programme_title_text(p)) for p in old_list)
            new_len = sum(len(get_programme_title_text(p)) for p in new_list)
            if new_len > old_len:
                merged_by_day[day] = new_list
                day_source_is_new[day] = True
            else:
                merged_by_day[day] = old_list
                day_source_is_new[day] = False

    merged_programmes = []
    for day in sorted(merged_by_day.keys()):
        merged_programmes.extend(merged_by_day[day])

    return merged_programmes, day_source_is_new


def load_aliases():
    alias_map = {}
    regex_aliases = []

    if not os.path.exists('alias.txt'):
        return alias_map, regex_aliases

    with open('alias.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',') if p.strip()]
            if not parts:
                continue
            main_name = transform2_zh_hans(process_display_name(parts[0]))
            alias_map[main_name] = main_name
            for alias in parts[1:]:
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        regex_aliases.append((re.compile(pattern), main_name))
                    except re.error as e:
                        print(f"alias.txt 正则错误: {pattern}, {e}")
                else:
                    alias_norm = transform2_zh_hans(process_display_name(alias))
                    alias_map[alias_norm] = main_name

    return alias_map, regex_aliases


def normalize_channel_name(name, alias_map, regex_aliases):
    if not name:
        return name
    name = transform2_zh_hans(process_display_name(name))
    if name in alias_map:
        return alias_map[name]
    for pattern, main_name in regex_aliases:
        if pattern.match(name):
            return main_name
    return name


def remap_display_names_for_output(main_name, existing_display_names, alias_map):
    """
    输出时重新映射 alias.txt 中的普通别名（不包含 regex 别名）
    """
    result = []
    seen = set()

    # 先保留已有 display-name
    for name, lang in existing_display_names:
        if name not in seen:
            result.append([name, lang])
            seen.add(name)

    # 再追加 alias.txt 中该主名对应的非正则别名
    for alias, mapped_main in alias_map.items():
        if alias == mapped_main and alias == main_name:
            continue
        if mapped_main == main_name and alias not in seen:
            result.append([alias, 'zh'])
            seen.add(alias)

    # 确保主名在内
    if main_name not in seen:
        result.insert(0, [main_name, 'zh'])

    return result


def parse_epg(epg_content, alias_map, regex_aliases):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    channel_id_to_main_name = {}

    for channel in root.findall('channel'):
        raw_channel_id = transform2_zh_hans(channel.get('id') or '')
        normalized_channel_id = normalize_channel_name(raw_channel_id, alias_map, regex_aliases)

        channel_display_names = []
        display_name_candidates = []

        for name in channel.findall('display-name'):
            text = name.text or ''
            t_name = transform2_zh_hans(text)
            t_name = process_display_name(t_name)
            normalized_name = normalize_channel_name(t_name, alias_map, regex_aliases)
            channel_display_names.append([normalized_name, name.get('lang', 'zh')])
            display_name_candidates.append(normalized_name)

        if normalized_channel_id:
            display_name_candidates.insert(0, normalized_channel_id)

        # 选主名：优先第一个可用名称
        main_name = None
        for candidate in display_name_candidates:
            if candidate:
                main_name = candidate
                break

        if not main_name:
            continue

        # display-name 去重
        deduped_names = []
        seen_names = set()
        for dn, lang in channel_display_names:
            if dn and dn not in seen_names:
                deduped_names.append([dn, lang])
                seen_names.add(dn[0])

        if main_name not in seen_names:
            deduped_names.insert(0, [main_name, 'zh'])

        channels[main_name] = deduped_names
        if raw_channel_id:
            channel_id_to_main_name[raw_channel_id] = main_name
        channel_id_to_main_name[normalized_channel_id] = main_name
        for candidate in display_name_candidates:
            if candidate:
                channel_id_to_main_name[candidate] = main_name

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        raw_channel_id = transform2_zh_hans(programme.get('channel') or '')
        channel_id = normalize_channel_name(raw_channel_id, alias_map, regex_aliases)
        channel_id = channel_id_to_main_name.get(channel_id, channel_id_to_main_name.get(raw_channel_id, channel_id))

        start_raw = programme.get("start")
        stop_raw = programme.get("stop")
        channel_start = parse_xmltv_datetime(start_raw)
        channel_stop = parse_xmltv_datetime(stop_raw)

        if channel_start is None or channel_stop is None:
            continue

        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        # 先检查 title 是否是广告主页节目，若是则整个 programme 丢弃
        titles_raw = programme.findall('title')
        should_skip = False
        for title in titles_raw:
            title_text = (title.text or '').strip()
            title_lang = title.get('lang')
            if title_lang == 'zh' or title_lang is None:
                title_text = transform2_zh_hans(title_text)
            if should_filter_programme_title(title_text):
                should_skip = True
                break
        if should_skip:
            continue

        channel_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")
            }
        )

        has_valid_title = False
        for title in titles_raw:
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)

            if should_filter_programme_title(channel_title):
                has_valid_title = False
                break

            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)
            has_valid_title = True

        if not has_valid_title:
            continue

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

    channels = {k: v for k, v in channels.items() if k in valid_channels and k in programmes}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})
    for channel_id in channels_id:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
        for prog in programmes[channel_id]:
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
    non_whitelist_urls = []
    whitelist_urls = []
    in_whitelist = False

    with open('config.txt', 'r', encoding='utf--8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line == '[WHITELIST]':
                in_whitelist = True
                continue
            if in_whitelist:
                whitelist_urls.append(line)
            else:
                non_whitelist_urls.append(line)

    return non_whitelist_urls, whitelist_urls


def write_source_log(source_log_map):
    log_path = 'epg_source.log'
    with open(log_path, 'w', encoding='utf-8') as f:
        for channel_name in sorted(source_log_map.keys()):
            for day in sorted(source_log_map[channel_name].keys()):
                source_url = source_log_map[channel_name][day]
                f.write(f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')}| 来源: {source_url}\n")


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


async def main():
    alias_map, regex_aliases = load_aliases()
    non_whitelist_urls, whitelist_urls = get_urls()

    url_items = []
    for url in non_whitelist_urls:
        url_items.append((url, False))
    for url in whitelist_urls:
        url_items.append((url, True))

    tasks = [fetch_epg(url) for url, _ in url_items]

    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)
    all_channel_source_type = {}  # channel -> True(white) / False(non-white)
    source_log_map = defaultdict(dict)  # channel -> {day: source_url}

    print("Finished.")

    i = 0
    for (source_url, is_whitelist), epg_content in zip(url_items, epg_contents):
        i += 1
        print(f"Processing EPG source...{i}/{len(epg_contents)}")
        if epg_content is None:
            continue

        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content, alias_map, regex_aliases)
        print("Finished.")

        with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
            for channel_id, display_names in channels.items():
                if channel_id not in all_channel_id:
                    all_channel_id.add(channel_id)
                    all_channel_names[channel_id] = display_names
                    all_programmes[channel_id] = programmes[channel_id]
                    all_channel_source_type[channel_id] = is_whitelist

                    # 初始化日志
                    for prog in programmes[channel_id]:
                        day = get_programme_day_key(prog)
                        if day is not None:
                            source_log_map[channel_id][day] = source_url
                else:
                    # 合并 display-name
                    existing_names = {name for name, _ in all_channel_names[channel_id]}
                    for dn in display_names:
                        if dn[0] not in existing_names:
                            all_channel_names[channel_id].append(dn)
                            existing_names.add(dn[0])

                    existing_is_whitelist = all_channel_source_type.get(channel_id, False)

                    # 规则：
                    # 1. 白名单 vs 非白名单：保留白名单
                    # 2. 白名单 vs 白名单：全部保留，按天新来源覆盖旧来源
                    # 3. 非白名单 vs 非白名单：按每天 title 总长度比较
                    if existing_is_whitelist and not is_whitelist:
                        pass
                    elif not existing_is_whitelist and is_whitelist:
                        merged_programmes, day_source_is_new = merge_programmes_by_day(
                            all_programmes[channel_id],
                            programmes[channel_id],
                            prefer_new=True
                        )
                        all_programmes[channel_id] = merged_programmes
                        all_channel_source_type[channel_id] = True
                        for day, used_new in day_source_is_new.items():
                            if used_new:
                                source_log_map[channel_id][day] = source_url
                    elif existing_is_whitelist and is_whitelist:
                        merged_programmes, day_source_is_new = merge_programmes_by_day(
                            all_programmes[channel_id],
                            programmes[channel_id],
                            prefer_new=True
                        )
                        all_programmes[channel_id] = merged_programmes
                        for day, used_new in day_source_is_new.items():
                            if used_new:
                                source_log_map[channel_id][day] = source_url
                    else:
                        merged_programmes, day_source_is_new = merge_programmes_by_day(
                            all_programmes[channel_id],
                            programmes[channel_id],
                            prefer_new=False
                        )
                        all_programmes[channel_id] = merged_programmes
                        for day, used_new in day_source_is_new.items():
                            if used_new:
                                source_log_map[channel_id][day] = source_url

                pbar.update(1)

    # 输出前重新映射 alias.txt 中的普通别名
    for channel_id in list(all_channel_id):
        all_channel_names[channel_id] = remap_display_names_for_output(
            channel_id,
            all_channel_names[channel_id],
            alias_map
        )

    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names, all_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')

    print("Writing source log...")
    write_source_log(source_log_map)
    print("Done.")


if __name__ == '__main__':
    asyncio.run(main())
