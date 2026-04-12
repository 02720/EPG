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

# 1. 全局 OpenCC 实例，避免重复创建
_cc = OpenCC("t2s")


def transform2_zh_hans(string):
    return _cc.convert(string)


def is_foreign_text(text):
    """检查文本是否为纯外文（不含中文字符）"""
    if not text:
        return False
    return not bool(re.search(r'[\u4e00-\u9fff]', text))


def process_display_name(display_name):
    """7. 去除'高清'但不去除'超高清'"""
    if display_name.endswith('高清') and not display_name.endswith('超高清'):
        display_name = display_name[:-2]
    return display_name


def parse_time_str(time_str):
    """3. 解析时间字符串，兼容多种格式"""
    if not time_str or not time_str.strip():
        return None
    time_str = re.sub(r'\s+', '', time_str)
    for fmt in ("%Y%m%d%H%M%S%z", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    return None


def is_advertisement_title(title):
    """2. 检查节目标题是否为广告/网站推广"""
    if title is None:
        return False
    title_stripped = title.strip()
    if re.search(r'由\s*https?://\S+', title_stripped):
        return True
    if re.match(r'^https?://\S+$', title_stripped):
        return True
    return False


def load_aliases(alias_file='alias.txt'):
    """6. 加载频道别名映射"""
    aliases = {}
    regex_aliases = []
    if not os.path.exists(alias_file):
        return aliases, regex_aliases
    with open(alias_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                continue
            main_name = parts[0]
            for alias in parts[1:]:
                if alias.startswith('re:'):
                    try:
                        regex_aliases.append((re.compile(alias[3:]), main_name))
                    except re.error:
                        pass
                else:
                    aliases[alias] = main_name
    return aliases, regex_aliases


def resolve_alias(name, aliases, regex_aliases):
    """6. 将别名解析为主名"""
    if name in aliases:
        return aliases[name]
    for pattern, main_name in regex_aliases:
        if pattern.search(name):
            return main_name
    return name


def load_alias_groups(alias_file='alias.txt'):
    """6. 加载别名组用于输出重新映射（仅非正则别名）"""
    groups = []
    if not os.path.exists(alias_file):
        return groups
    with open(alias_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                continue
            main_name = parts[0]
            plain_aliases = [a for a in parts[1:] if not a.startswith('re:')]
            if plain_aliases:
                groups.append((main_name, plain_aliases))
    return groups


def get_urls_with_whitelist(config_file='config.txt'):
    """8. 解析 config.txt，返回 [(url, is_whitelist), ...]"""
    result = []
    in_whitelist = False
    with open(config_file, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line == '[WHITELIST]':
                in_whitelist = True
                continue
            result.append((line, in_whitelist))
    return result


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


def parse_epg(epg_content):
    """解析EPG XML内容，返回 (channels, programmes)"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)
    valid_channels = set()
    today = datetime.now(TZ_UTC_PLUS_8).date()

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id'))
        channel_display_names = []
        for name in channel.findall('display-name'):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get('lang', 'zh')])
        if not channel_id.isdigit() and channel_id not in [d[0] for d in channel_display_names]:
            channel_display_names.append([channel_id, 'zh'])
        channels[channel_id] = channel_display_names

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel'))
        raw_start = programme.get('start', '')
        raw_stop = programme.get('stop', '')

        # 3. 修复时间解析
        start_dt = parse_time_str(raw_start)
        stop_dt = parse_time_str(raw_stop)

        if start_dt is None or stop_dt is None:
            continue

        if start_dt.tzinfo:
            start_dt_local = start_dt.astimezone(TZ_UTC_PLUS_8)
        else:
            start_dt_local = start_dt.replace(tzinfo=TZ_UTC_PLUS_8)
        if stop_dt.tzinfo:
            stop_dt_local = stop_dt.astimezone(TZ_UTC_PLUS_8)
        else:
            stop_dt_local = stop_dt.replace(tzinfo=TZ_UTC_PLUS_8)

        if stop_dt_local.date() == today:
            valid_channels.add(channel_id)

        channel_elem = ET.Element('programme', attrib={
            "start": start_dt_local.strftime("%Y%m%d%H%M%S %z"),
            "stop": stop_dt_local.strftime("%Y%m%d%H%M%S %z")
        })

        # 2. 过滤广告标题
        has_valid_title = False
        for title in programme.findall('title'):
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()

            if is_advertisement_title(channel_title):
                continue

            has_valid_title = True
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(channel_elem, 'title')
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set('lang', langattr)

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

        programmes[channel_id].append((start_dt_local.date(), channel_elem))

    channels = {k: v for k, v in channels.items() if k in valid_channels}
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
            display_name_elem = ET.SubElement(
                channel_elem, 'display-name', attrib={"lang": langattr})
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


async def main():
    urls_info = get_urls_with_whitelist()
    urls = [url for url, _ in urls_info]
    whitelist_urls = set(url for url, is_wl in urls_info if is_wl)

    aliases, regex_aliases = load_aliases()
    alias_groups = load_alias_groups()

    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished.")

    # 数据收集: channel_data[resolved_name][source_url][date] = [prog_elements]
    channel_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    channel_display = {}
    name_to_resolved = {}

    i = 0
    for idx, epg_content in enumerate(epg_contents):
        url = urls[idx]
        i += 1
        print(f"Processing EPG source...{i}/{len(epg_contents)}")
        if epg_content is None:
            continue
        print("Parsing EPG data...")
        channels, programmes = parse_epg(epg_content)
        print("Finished.")

        with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
            for channel_id, display_names in channels.items():
                if channel_id not in programmes or len(programmes[channel_id]) == 0:
                    pbar.update(1)
                    continue

                # 6. 别名解析：先将别名转换为主名
                resolved_name = None
                all_names = [channel_id] + [dn[0] for dn in display_names]
                for name in all_names:
                    r = resolve_alias(name, aliases, regex_aliases)
                    if r != name:
                        resolved_name = r
                        break

                # 检查是否已有名称映射
                if resolved_name is None:
                    for name in all_names:
                        if name in name_to_resolved:
                            resolved_name = name_to_resolved[name]
                            break

                if resolved_name is None:
                    resolved_name = display_names[0][0] if display_names else channel_id

                # 存储显示名称（首次遇到时）
                if resolved_name not in channel_display:
                    channel_display[resolved_name] = list(display_names)

                # 建立名称映射
                for name in all_names:
                    if name not in name_to_resolved:
                        name_to_resolved[name] = resolved_name

                # 按来源和日期存储节目
                for date, prog_elem in programmes[channel_id]:
                    channel_data[resolved_name][url][date].append(prog_elem)

                pbar.update(1)

    # 4 & 8. 按天比较节目标题总长度并合并，白名单优先
    final_programmes = defaultdict(list)
    log_data = []

    for channel_name, sources in channel_data.items():
        all_dates = set()
        for src_url, date_data in sources.items():
            all_dates.update(date_data.keys())

        for date in sorted(all_dates):
            source_options = []
            for src_url, date_data in sources.items():
                if date not in date_data:
                    continue
                progs = date_data[date]
                # 4. 比较标题总长度（忽略desc）
                total_title_len = sum(
                    len(t.text.strip()) if t.text else 0
                    for p in progs for t in p.findall('title')
                )
                is_wl = src_url in whitelist_urls
                source_options.append((src_url, is_wl, total_title_len, progs))

            if not source_options:
                continue

            # 8. 白名单数据源优先，不经过比较直接保留
            wl_options = [o for o in source_options if o[1]]
            if wl_options:
                best = max(wl_options, key=lambda x: x[2])
            else:
                best = max(source_options, key=lambda x: x[2])

            final_programmes[channel_name].extend(best[3])
            log_data.append((channel_name, date.strftime('%Y-%m-%d'), best[0]))

    # 构建输出频道名称
    output_names = defaultdict(list)
    for ch_name, display_names in channel_display.items():
        output_names[ch_name] = list(display_names)
        # 确保主名在显示名称中
        if ch_name not in [d[0] for d in output_names[ch_name]]:
            output_names[ch_name].append([ch_name, 'zh'])

    # 6. 重新映射别名（非正则表达式别名作为额外的 display-name）
    for main_name, plain_aliases in alias_groups:
        if main_name in output_names:
            existing = {dn[0] for dn in output_names[main_name]}
            for alias in plain_aliases:
                if alias not in existing:
                    output_names[main_name].append([alias, 'zh'])
                    existing.add(alias)

    # 9. 去除国外EPG
    channel_ids = list(output_names.keys())
    filtered = set()
    for ch_name in channel_ids:
        # 频道名为纯外文
        if is_foreign_text(ch_name):
            print(f"Removing foreign channel: {ch_name}")
            filtered.add(ch_name)
            continue
        # 节目标题超过60%为外文
        progs = final_programmes.get(ch_name, [])
        if progs:
            total = 0
            foreign = 0
            for p in progs:
                for t in p.findall('title'):
                    if t.text:
                        total += 1
                        if is_foreign_text(t.text):
                            foreign += 1
            if total > 0 and foreign / total > 0.6:
                print(f"Removing channel >60% foreign titles: {ch_name}")
                filtered.add(ch_name)

    for ch in filtered:
        del output_names[ch]
        if ch in final_programmes:
            del final_programmes[ch]

    channel_ids = [ch for ch in channel_ids if ch not in filtered]

    # 5. 过滤日志（仅保留最终输出中的频道）
    remaining = set(channel_ids)
    log_data_filtered = [(ch, d, s) for ch, d, s in log_data if ch in remaining]
    log_data_filtered.sort(key=lambda x: (x[0], x[1]))

    print("Writing to XML...")
    write_to_xml(channel_ids, output_names, final_programmes, 'output/epg.xml')
    compress_to_gz('output/epg.xml', 'output/epg.gz')

    # 5. 写入日志
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(script_dir, 'epg_source.log')
    with open(log_path, 'w', encoding='utf-8') as f:
        for ch, d, s in log_data_filtered:
            f.write(f"频道: {ch} | 日期: {d} | 来源: {s}\n")

    print("Done.")


if __name__ == '__main__':
    asyncio.run(main())
