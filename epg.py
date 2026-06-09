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
import logging
from tqdm import tqdm

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# ========== 优化1：全局 OpenCC，避免重复创建 ==========
CC = OpenCC("t2s")

# ========== 优化5：日志功能 ==========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, 'epg_source.log')

epg_logger = logging.getLogger('epg_source')
epg_logger.setLevel(logging.INFO)
# 避免重复添加 handler
if not epg_logger.handlers:
    _fh = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    _fh.setFormatter(logging.Formatter('%(message)s'))
    epg_logger.addHandler(_fh)

# ========== 优化2：需要屏蔽的节目主页关键词 ==========
BLOCKED_TITLE_KEYWORDS = [
    'https://epg.136605.xyz',
    '由https://epg.136605.xyz提供服务',
]


def transform2_zh_hans(string):
    if string is None:
        return string
    return CC.convert(string)


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


# ========== 优化7：去除“高清”，但保留“超高清” ==========
def process_display_name(display_name):
    if display_name is None:
        return display_name
    # 仅当结尾是“高清”且不是“超高清”时才去除
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name


# ========== 优化3：安全的时间解析 ==========
def safe_parse_time(time_str):
    """
    解析 EPG 时间，兼容：
    - 空字符串
    - 带时区 %Y%m%d%H%M%S%z
    - 不带时区 %Y%m%d%H%M%S（默认按 +0800 处理）
    返回 datetime（UTC+8）或 None
    """
    if not time_str:
        return None
    s = re.sub(r'\s+', '', time_str.strip())
    if not s:
        return None
    # 先尝试带时区
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        pass
    # 再尝试不带时区
    try:
        dt = datetime.strptime(s, "%Y%m%d%H%M%S")
        dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)
        return dt
    except ValueError:
        return None


# ========== 优化6：频道别名功能 ==========
def load_aliases():
    """
    读取 alias.txt
    返回:
      exact_alias_map: {别名(简体,处理后): 主名}
      regex_alias_list: [(compiled_regex, 主名), ...]
      main_to_aliases: {主名: [普通别名列表(非正则)]}  用于输出时重新映射
    """
    exact_alias_map = {}
    regex_alias_list = []
    main_to_aliases = defaultdict(list)

    alias_path = os.path.join(SCRIPT_DIR, 'alias.txt')
    if not os.path.exists(alias_path):
        return exact_alias_map, regex_alias_list, main_to_aliases

    with open(alias_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                continue
            main_name = transform2_zh_hans(parts[0])
            main_name = process_display_name(main_name)
            # 主名自身也映射到自身
            exact_alias_map[main_name] = main_name
            for alias in parts[1:]:
                if not alias:
                    continue
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        regex_alias_list.append((re.compile(pattern), main_name))
                    except re.error as e:
                        print(f"正则编译失败: {pattern} -> {e}")
                else:
                    alias_norm = transform2_zh_hans(alias)
                    alias_norm = process_display_name(alias_norm)
                    exact_alias_map[alias_norm] = main_name
                    main_to_aliases[main_name].append(alias_norm)
    return exact_alias_map, regex_alias_list, main_to_aliases


def resolve_alias(name, exact_alias_map, regex_alias_list):
    """将一个显示名解析为主名，匹配不到则返回原名"""
    if name in exact_alias_map:
        return exact_alias_map[name]
    for regex, main_name in regex_alias_list:
        if regex.match(name):
            return main_name
    return None


def parse_epg(epg_content, exact_alias_map, regex_alias_list):
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    # ===== 优化6：别名转换，先建立 原channel_id -> 主名(channel映射) =====
    # channel_id_remap: 原id -> 转换后的id(主名 或 原id)
    channel_id_remap = {}

    for channel in root.findall('channel'):
        raw_id = channel.get('id')
        channel_id = transform2_zh_hans(raw_id)
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [d[0] for d in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])

        # 别名解析：尝试用 channel_id 或任意 display-name 解析主名
        resolved_main = resolve_alias(channel_id, exact_alias_map, regex_alias_list)
        if resolved_main is None:
            for d in channel_display_names:
                resolved_main = resolve_alias(d[0], exact_alias_map, regex_alias_list)
                if resolved_main is not None:
                    break

        if resolved_main is not None:
            new_id = resolved_main
            # 确保主名出现在 display-name 中
            if new_id not in [d[0] for d in channel_display_names]:
                channel_display_names.insert(0, [new_id, 'zh'])
        else:
            new_id = channel_id

        channel_id_remap[channel_id] = new_id
        channels[new_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        raw_channel = programme.get('channel')
        channel_id = transform2_zh_hans(raw_channel)
        # 应用别名重映射
        channel_id = channel_id_remap.get(channel_id, channel_id)
        # 若 channel 没有对应 channel 元素，也尝试别名解析
        if channel_id not in channels:
            resolved = resolve_alias(channel_id, exact_alias_map, regex_alias_list)
            if resolved is not None:
                channel_id = resolved

        # ===== 优化3：安全解析时间 =====
        channel_start = safe_parse_time(programme.get('start'))
        channel_stop = safe_parse_time(programme.get('stop'))
        if channel_start is None or channel_stop is None:
            # 时间无效，跳过该节目
            continue

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        channel_elem = ET.Element(
            'programme',
            attrib={"start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                    "stop": channel_stop.strftime("%Y%m%d%H%M%S %z")})

        # ===== 优化2：屏蔽网站主页节目 =====
        skip_programme = False
        for title in programme.findall('title'):
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()
            # 检查是否为屏蔽内容
            for kw in BLOCKED_TITLE_KEYWORDS:
                if kw in channel_title:
                    skip_programme = True
                    break
            if skip_programme:
                break
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)

        if skip_programme:
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

        # 记录该节目所属日期（按开始时间）用于合并比较
        channel_elem.set('_day', channel_start.strftime("%Y-%m-%d"))
        programmes[channel_id].append(channel_elem)

    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


# ========== 优化4 & 优化5：按天比较 title 总长度，并记录来源 ==========
def group_programmes_by_day(prog_list):
    """将一个频道的节目列表按天分组"""
    day_map = defaultdict(list)
    for prog in prog_list:
        day = prog.get('_day')
        if day is None:
            start = prog.get('start', '')
            day = start[:8] if len(start) >= 8 else 'unknown'
        day_map[day].append(prog)
    return day_map


def day_title_length(prog_list):
    """计算一天内所有节目 title 文本长度之和"""
    total = 0
    for prog in prog_list:
        for title in prog.findall('title'):
            if title.text:
                total += len(title.text)
    return total


def get_main_display_name(display_names, channel_id):
    """获取频道主名(主显示名)，没有则用 channel_id"""
    if display_names:
        return display_names[0][0]
    return channel_id


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
            display_name_elem = ET.SubElement(
                channel_elem, 'display-name', attrib={"lang": langattr})
            display_name_elem.text = display_name
        for prog in programmes[channel_id]:
            prog.set('channel', channel_id)
            # 移除临时属性
            if '_day' in prog.attrib:
                del prog.attrib['_day']
            root.append(prog)

    rough_string = ET.tostring(root, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(reparsed.toprettyxml(indent='\t', newl='\n'))


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


# ========== 优化8：读取 config，区分白名单与非白名单 ==========
def get_urls():
    """
    返回 (non_whitelist_urls, whitelist_urls)
    [WHITELIST] 以上为非白名单，以下为白名单
    """
    non_whitelist = []
    whitelist = []
    in_whitelist = False
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.upper() == '[WHITELIST]':
                in_whitelist = True
                continue
            if in_whitelist:
                whitelist.append(line)
            else:
                non_whitelist.append(line)
    return non_whitelist, whitelist


# ===== 合并核心逻辑 =====
def merge_source(epg_content, url, is_whitelist,
                 exact_alias_map, regex_alias_list,
                 all_channel_id, all_channel_names, all_programmes,
                 all_channels_map,
                 # 记录每个频道-每天 的来源与白名单标记和title长度
                 day_source_info):
    """
    day_source_info: {map_id: {day: {'len': int, 'url': str, 'whitelist': bool}}}
    all_programmes 按 map_id 存储 {day: [progs]} 结构（内部用 dict 暂存便于按天替换）
    """
    if epg_content is None:
        return

    print("Parsing EPG data...")
    channels, programmes = parse_epg(epg_content, exact_alias_map, regex_alias_list)
    print("Finished.")

    with tqdm(total=len(channels), desc="Merging EPG", unit="ch") as pbar:
        for channel_id, display_names in channels.items():
            prog_list = programmes.get(channel_id, [])
            if len(prog_list) == 0:
                pbar.update(1)
                continue

            # 寻找映射主 id
            is_in_map = False
            map_id = channel_id
            for display_name_node in display_names:
                dn = display_name_node[0]
                if dn in all_channels_map:
                    map_id = all_channels_map[dn]
                    is_in_map = True
                    break
            if not is_in_map and channel_id in all_channels_map:
                map_id = all_channels_map[channel_id]
                is_in_map = True

            if not is_in_map:
                map_id = channel_id
                all_channel_id.add(map_id)
                all_channel_names[map_id] = display_names
                all_programmes[map_id] = {}  # {day: [progs]}
                day_source_info[map_id] = {}
                for display_name_node in display_names:
                    all_channels_map[display_name_node[0]] = map_id
            else:
                # 补充别名
                for display_name_node in display_names:
                    dn = display_name_node[0]
                    if dn not in all_channels_map:
                        all_channel_names[map_id].append(display_name_node)
                        all_channels_map[dn] = map_id

            main_name = get_main_display_name(all_channel_names[map_id], map_id)

            # 按天分组
            day_groups = group_programmes_by_day(prog_list)
            existing = all_programmes.setdefault(map_id, {})
            existing_info = day_source_info.setdefault(map_id, {})

            for day, progs in day_groups.items():
                new_len = day_title_length(progs)
                prev = existing_info.get(day)

                if prev is None:
                    # 该天还没有数据，直接采用
                    existing[day] = progs
                    existing_info[day] = {'len': new_len, 'url': url, 'whitelist': is_whitelist}
                else:
                    prev_whitelist = prev['whitelist']
                    if prev_whitelist and not is_whitelist:
                        # 已有白名单数据，非白名单不可覆盖
                        continue
                    elif not prev_whitelist and is_whitelist:
                        # 白名单覆盖非白名单
                        existing[day] = progs
                        existing_info[day] = {'len': new_len, 'url': url, 'whitelist': is_whitelist}
                    else:
                        # 同级别（都白名单 或 都非白名单），比较 title 总长度
                        # 白名单要求全部保留：但同一频道同一天只能保留一份，长者优先
                        if new_len > prev['len']:
                            existing[day] = progs
                            existing_info[day] = {'len': new_len, 'url': url, 'whitelist': is_whitelist}

            pbar.update(1)


def main_sync(epg_contents, non_whitelist_urls, whitelist_urls):
    exact_alias_map, regex_alias_list, main_to_aliases = load_aliases()

    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(dict)   # {map_id: {day: [progs]}}
    all_channels_map = {}
    day_source_info = {}                 # {map_id: {day: {...}}}

    # epg_contents 顺序 = non_whitelist_urls + whitelist_urls
    all_urls = non_whitelist_urls + whitelist_urls
    total = len(epg_contents)

    for i, epg_content in enumerate(epg_contents):
        url = all_urls[i] if i < len(all_urls) else ""
        is_whitelist = i >= len(non_whitelist_urls)
        print(f"Processing EPG source...{i + 1}/{total} "
              f"({'WHITELIST' if is_whitelist else 'NORMAL'}) {url}")
        merge_source(epg_content, url, is_whitelist,
                     exact_alias_map, regex_alias_list,
                     all_channel_id, all_channel_names, all_programmes,
                     all_channels_map, day_source_info)

    # ===== 优化5：写日志 =====
    print("Writing log...")
    for map_id in sorted(all_channel_id):
        main_name = get_main_display_name(all_channel_names[map_id], map_id)
        info_days = day_source_info.get(map_id, {})
        for day in sorted(info_days.keys()):
            info = info_days[day]
            # day 格式可能是 %Y-%m-%d 或 %Y%m%d
            day_fmt = day
            if re.fullmatch(r'\d{8}', day):
                day_fmt = f"{day[:4]}-{day[4:6]}-{day[6:8]}"
            epg_logger.info(
                f"频道: [{main_name}] | 日期: {day_fmt} | 来源: {info['url']}")

    # ===== 优化6：输出前重新映射别名（普通别名，非正则） =====
    for map_id in all_channel_id:
        main_name = get_main_display_name(all_channel_names[map_id], map_id)
        if main_name in main_to_aliases:
            existing_names = set(d[0] for d in all_channel_names[map_id])
            for alias in main_to_aliases[main_name]:
                if alias not in existing_names:
                    all_channel_names[map_id].append([alias, 'zh'])
                    existing_names.add(alias)

    # 将 {day: [progs]} 展平为列表
    flat_programmes = defaultdict(list)
    for map_id, day_map in all_programmes.items():
        for day in sorted(day_map.keys()):
            flat_programmes[map_id].extend(day_map[day])

    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names,
                 flat_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')


async def main():
    non_whitelist_urls, whitelist_urls = get_urls()
    all_urls = non_whitelist_urls + whitelist_urls
    tasks = [fetch_epg(url) for url in all_urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished.")
    main_sync(epg_contents, non_whitelist_urls, whitelist_urls)


if __name__ == '__main__':
    asyncio.run(main())
