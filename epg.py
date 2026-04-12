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

# 1) 全局 OpenCC，避免重复创建
_CC_T2S = OpenCC("t2s")

_URL_RE = re.compile(r"https?://[^\s<>'\"）)】\]]+")


def transform2_zh_hans(string):
    if string is None:
        return ""
    return _CC_T2S.convert(string)


def parse_epg_time(raw: str):
    # 3) 兼容空值、无时区、有/无空格、有/无冒号时区
    if raw is None:
        return None
    s = re.sub(r"\s+", "", raw)
    if not s:
        return None

    # Z 结尾视为 UTC
    if s.endswith("Z") and len(s) >= 15:
        s = s[:-1] + "+0000"

    # 尝试带时区
    try:
        # 允许 +0800 或 +08:00
        if re.search(r"([+-]\d{2}:?\d{2})$", s):
            dt = datetime.strptime(s, "%Y%m%d%H%M%S%z")
            return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        pass

    # 无时区：按 UTC+8
    try:
        dt = datetime.strptime(s[:14], "%Y%m%d%H%M%S")
        return dt.replace(tzinfo=TZ_UTC_PLUS_8)
    except ValueError:
        return None


def is_homepage_programme(raw_titles, raw_descs):
    # 2) 屏蔽插入主页/服务信息的“节目单”
    joined = " ".join([x for x in (raw_titles + raw_descs) if x]).strip()
    if not joined:
        return False
    if "提供服务" in joined and _URL_RE.search(joined):
        return True
    # 标题/描述里直接出现 URL（多数为“站点主页插入”）
    if _URL_RE.search(joined) and len(joined) <= 200:
        return True
    # 纯 URL
    if _URL_RE.fullmatch(joined):
        return True
    return False


def process_display_name(display_name):
    # 7) 可以去除“高清”，但不要去除“超高清”
    if display_name.endswith("高清") and not display_name.endswith("超高清"):
        display_name = display_name[:-2]
    return display_name


def load_aliases(alias_path="alias.txt"):
    literal_to_main = {}
    regex_aliases = []  # list[(main, compiled_regex)]
    main_to_literal_aliases = defaultdict(list)

    if not os.path.exists(alias_path):
        return literal_to_main, regex_aliases, main_to_literal_aliases

    with open(alias_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",") if p.strip()]
            if not parts:
                continue
            main = parts[0]
            literal_to_main[main] = main  # 主名本身
            for alias in parts[1:]:
                if alias.startswith("re:"):
                    pat = alias[3:].strip()
                    if not pat:
                        continue
                    try:
                        regex_aliases.append((main, re.compile(pat)))
                    except re.error:
                        continue
                else:
                    literal_to_main[alias] = main
                    main_to_literal_aliases[main].append(alias)

    return literal_to_main, regex_aliases, main_to_literal_aliases


def canonicalize_name(name, literal_to_main, regex_aliases):
    if name is None:
        return ""
    raw = name
    s = raw.strip()
    if not s:
        return s
    if s in literal_to_main:
        return literal_to_main[s]
    for main, cre in regex_aliases:
        try:
            if cre.search(raw):
                return main
        except re.error:
            continue
    return s


def normalize_by_alias(channels, programmes, literal_to_main, regex_aliases):
    # 6) 获取源后先把别名统一为主名（含 channel-id 与 display-name）
    new_channels = {}
    new_programmes = defaultdict(list)

    for old_channel_id, display_names in channels.items():
        canon_id = canonicalize_name(old_channel_id, literal_to_main, regex_aliases)

        # display-name 规范化 + 去重 + 主名置前
        seen = set()
        norm_names = []
        for name, lang in display_names:
            cn = canonicalize_name(name, literal_to_main, regex_aliases)
            key = (cn, lang)
            if key in seen:
                continue
            seen.add(key)
            norm_names.append([cn, lang])

        if canon_id:
            # 确保主名存在且尽量放前
            if (canon_id, "zh") not in seen:
                norm_names.insert(0, [canon_id, "zh"])
            else:
                for i, (n, l) in enumerate(norm_names):
                    if n == canon_id and l == "zh":
                        norm_names.insert(0, norm_names.pop(i))
                        break

        if canon_id not in new_channels:
            new_channels[canon_id] = norm_names
        else:
            existing = new_channels[canon_id]
            ex_seen = {(n, l) for n, l in existing}
            for n, l in norm_names:
                if (n, l) not in ex_seen:
                    existing.append([n, l])
                    ex_seen.add((n, l))

        new_programmes[canon_id].extend(programmes.get(old_channel_id, []))

    return new_channels, new_programmes


def parse_epg(epg_content):
    try:
        parser = ET.XMLParser(encoding="UTF-8")
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML: {e}")
        print(f"Problematic content: {epg_content[:500]}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall("channel"):
        channel_id = transform2_zh_hans(channel.get("id"))
        channel_display_names = []
        for name in channel.findall("display-name"):
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            channel_display_names.append([t_name, name.get("lang", "zh")])
        if not channel_id.isdigit() and channel_id not in channel_display_names:
            channel_display_names.append([channel_id, "zh"])
        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall("programme"):
        channel_id = transform2_zh_hans(programme.get("channel"))

        raw_titles = [(t.text or "").strip() for t in programme.findall("title")]
        raw_descs = [(d.text or "").strip() for d in programme.findall("desc")]
        if is_homepage_programme(raw_titles, raw_descs):
            continue

        channel_start = parse_epg_time(programme.get("start"))
        channel_stop = parse_epg_time(programme.get("stop"))
        if channel_start is None or channel_stop is None:
            continue

        if channel_stop.date() == today:
            valid_channels.add(channel_id)

        channel_elem = ET.SubElement(
            root,
            "programme",
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
            },
        )
        for title in programme.findall("title"):
            if title.text is None:
                channel_title = "精彩节目"
            else:
                channel_title = title.text.strip()
            langattr = title.get("lang")
            if langattr == "zh" or langattr is None:
                channel_title = transform2_zh_hans(channel_title)
            channel_elem_t = ET.SubElement(channel_elem, "title")
            channel_elem_t.text = channel_title
            if langattr is not None:
                channel_elem_t.set("lang", langattr)

        for desc in programme.findall("desc"):
            if desc.text is None:
                continue
            langattr = desc.get("lang")
            channel_desc = desc.text.strip()
            if langattr == "zh" or langattr is None:
                channel_desc = transform2_zh_hans(channel_desc)
            channel_elem_d = ET.SubElement(channel_elem, "desc")
            channel_elem_d.text = channel_desc.strip()
            if langattr is not None:
                channel_elem_d.set("lang", langattr)

        programmes[channel_id].append(channel_elem)

    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}
    return channels, programmes


async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith(".gz"):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode("utf-8", errors="ignore")
                else:
                    return await response.text(encoding="utf-8")
    except aiohttp.ClientError as e:
        print(f"{url}HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print("{url}请求超时")
    except Exception as e:
        print(f"{url}其他错误: {e}")
    return None


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, "rb") as f_in:
        with gzip.open(output_filename, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    # 8) 支持 [WHITELIST] 分段
    non_whitelist = []
    whitelist = []
    in_whitelist = False
    with open("config.txt", "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line == "[WHITELIST]":
                in_whitelist = True
                continue
            if in_whitelist:
                whitelist.append(line)
            else:
                non_whitelist.append(line)
    return non_whitelist, whitelist


def split_programmes_by_day(programme_list):
    day_map = defaultdict(list)   # date -> list[prog]
    day_title_len = defaultdict(int)  # date -> total title length (ignore desc)
    for prog in programme_list:
        dt = parse_epg_time(prog.get("start"))
        if dt is None:
            continue
        day = dt.date()
        day_map[day].append(prog)
        total = 0
        for t in prog.findall("title"):
            if t.text:
                total += len(t.text.strip())
        day_title_len[day] += total
    return day_map, day_title_len


def choose_channel_name(display_name_nodes, fallback):
    if display_name_nodes:
        for n, lang in display_name_nodes:
            if lang == "zh" and n:
                return n
        for n, _ in display_name_nodes:
            if n:
                return n
    return fallback


def is_pure_foreign_name(name: str):
    if not name:
        return False
    has_cjk = bool(re.search(r"[\u4e00-\u9fff]", name))
    has_latin = bool(re.search(r"[A-Za-z]", name))
    return (not has_cjk) and has_latin


def is_foreign_title(title: str):
    if not title:
        return False
    has_cjk = bool(re.search(r"[\u4e00-\u9fff]", title))
    has_latin = bool(re.search(r"[A-Za-z]", title))
    return (not has_cjk) and has_latin


def filter_foreign_channels(all_channel_id, all_channel_names, final_programmes, merged_sources_by_day):
    # 9) 去除国外 EPG：频道名纯外文 或 title 超过 60% 外文
    to_remove = set()
    for cid in list(all_channel_id):
        ch_name = choose_channel_name(all_channel_names.get(cid, []), cid)
        if is_pure_foreign_name(ch_name):
            to_remove.add(cid)
            continue

        progs = final_programmes.get(cid, [])
        if not progs:
            continue

        foreign_cnt = 0
        total_cnt = 0
        for prog in progs:
            # 用该 programme 的第一个 title（若无则跳过）
            titles = [t.text.strip() for t in prog.findall("title") if t.text and t.text.strip()]
            if not titles:
                continue
            total_cnt += 1
            if is_foreign_title(titles[0]):
                foreign_cnt += 1

        if total_cnt > 0 and (foreign_cnt / total_cnt) > 0.6:
            to_remove.add(cid)

    for cid in to_remove:
        all_channel_id.discard(cid)
        all_channel_names.pop(cid, None)
        final_programmes.pop(cid, None)
        merged_sources_by_day.pop(cid, None)


def remap_literal_aliases_to_display_names(all_channel_names, main_to_literal_aliases):
    # 6) 输出阶段把别名（非正则）重新加回 display-name
    for cid, nodes in all_channel_names.items():
        existing = {n for n, _ in nodes}

        main = None
        if cid in main_to_literal_aliases:
            main = cid
        else:
            for n, _ in nodes:
                if n in main_to_literal_aliases:
                    main = n
                    break

        if not main:
            continue

        for alias in main_to_literal_aliases.get(main, []):
            if alias and alias not in existing:
                nodes.append([alias, "zh"])
                existing.add(alias)


def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists("output"):
        os.makedirs("output")
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element("tv", attrib={"date": current_time})
    for channel_id in channels_id:
        channel_elem = ET.SubElement(root, "channel", attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            display_name_elem = ET.SubElement(channel_elem, "display-name", attrib={"lang": langattr})
            display_name_elem.text = display_name
        for prog in programmes[channel_id]:
            prog.set("channel", channel_id)
            root.append(prog)

    rough_string = ET.tostring(root, "utf-8")
    reparsed = minidom.parseString(rough_string)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(reparsed.toprettyxml(indent="\t", newl="\n"))


async def main():
    non_whitelist_urls, whitelist_urls = get_urls()

    url_entries = [(u, False) for u in non_whitelist_urls] + [(u, True) for u in whitelist_urls]
    urls = [u for u, _ in url_entries]

    literal_to_main, regex_aliases, main_to_literal_aliases = load_aliases("alias.txt")

    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished.")

    all_channels_map = {}
    all_channel_id = set()
    all_channel_names = defaultdict(list)

    # programmes 分天存储：channel -> date -> list[programme]
    all_programmes_by_day = defaultdict(lambda: defaultdict(list))
    # 记录非白名单比较时每一天的 best_len：channel -> date -> int
    best_title_len_by_day = defaultdict(lambda: defaultdict(int))

    # 5) 日志：合成后每个频道每天来源
    merged_sources_by_day = defaultdict(lambda: defaultdict(set))  # channel -> date -> set(url)
    whitelisted_channels = set()

    def resolve_map_id(channel_id, display_names):
        for dn, _ in display_names:
            if dn in all_channels_map:
                return all_channels_map[dn], True
        if channel_id in all_channels_map:
            return all_channels_map[channel_id], True
        return channel_id, False

    def ensure_channel_registered(map_id, display_names):
        if map_id not in all_channel_id:
            all_channel_id.add(map_id)
            all_channel_names[map_id] = display_names
            for dn, _ in display_names:
                all_channels_map[dn] = map_id
        else:
            # 补充 display-name 与反向映射
            existing = all_channel_names[map_id]
            existing_set = {(n, l) for n, l in existing}
            for node in display_names:
                n, l = node
                if (n, l) not in existing_set:
                    existing.append(node)
                    existing_set.add((n, l))
                if n not in all_channels_map:
                    all_channels_map[n] = map_id

    def sort_programmes(progs):
        def keyf(p):
            dt = parse_epg_time(p.get("start"))
            if dt is None:
                return datetime.max.replace(tzinfo=TZ_UTC_PLUS_8)
            return dt
        return sorted(progs, key=keyf)

    # 先处理白名单，再处理非白名单（8：白名单优先）
    for pass_whitelist in (True, False):
        i = 0
        for (url, is_whitelist), epg_content in zip(url_entries, epg_contents):
            if is_whitelist != pass_whitelist:
                continue
            i += 1
            print(f"Processing EPG source ({'WHITELIST' if is_whitelist else 'NON-WHITELIST'})...{i}")
            if epg_content is None:
                continue

            print("Parsing EPG data...")
            channels, programmes = parse_epg(epg_content)

            # 6) 别名转换为主名
            channels, programmes = normalize_by_alias(channels, programmes, literal_to_main, regex_aliases)

            print("Finished.")

            with tqdm(total=len(channels), desc="Merging EPG", unit="file") as pbar:
                for channel_id, display_names in channels.items():
                    if channel_id not in programmes or len(programmes[channel_id]) == 0:
                        pbar.update(1)
                        continue

                    map_id, _ = resolve_map_id(channel_id, display_names)

                    # 如果该 channel 已被白名单占用，非白名单直接跳过
                    if (not is_whitelist) and (map_id in whitelisted_channels):
                        pbar.update(1)
                        continue

                    # 注册/补充 channel 名称映射
                    ensure_channel_registered(map_id, display_names)

                    # 分天计算 title 总长度
                    day_map, day_title_len = split_programmes_by_day(programmes[channel_id])

                    if is_whitelist:
                        # 若之前已有非白名单数据，则白名单覆盖
                        if map_id not in whitelisted_channels and map_id in all_programmes_by_day:
                            # 覆盖：清空非白名单选择结果
                            all_programmes_by_day[map_id].clear()
                            merged_sources_by_day[map_id].clear()
                            best_title_len_by_day[map_id].clear()

                        whitelisted_channels.add(map_id)

                        # 白名单：全部保留（不做第4点比较）
                        for day, progs in day_map.items():
                            all_programmes_by_day[map_id][day].extend(progs)
                            merged_sources_by_day[map_id][day].add(url)
                    else:
                        # 非白名单：按天比较 title 总长度，保留信息多的
                        for day, progs in day_map.items():
                            new_len = day_title_len.get(day, 0)
                            old_len = best_title_len_by_day[map_id].get(day, -1)
                            if new_len > old_len:
                                all_programmes_by_day[map_id][day] = progs
                                best_title_len_by_day[map_id][day] = new_len
                                merged_sources_by_day[map_id][day] = {url}
                            elif day not in all_programmes_by_day[map_id]:
                                all_programmes_by_day[map_id][day] = progs
                                best_title_len_by_day[map_id][day] = new_len
                                merged_sources_by_day[map_id][day] = {url}

                    pbar.update(1)

    # 组装最终 programmes（按天、按 start 排序）
    final_programmes = {}
    for cid in list(all_channel_id):
        if cid not in all_programmes_by_day:
            continue
        merged = []
        for day in sorted(all_programmes_by_day[cid].keys()):
            merged.extend(sort_programmes(all_programmes_by_day[cid][day]))
        final_programmes[cid] = merged

    # 6) 输出阶段重新映射别名（非正则）
    remap_literal_aliases_to_display_names(all_channel_names, main_to_literal_aliases)

    # 9) 去除国外 EPG 数据
    filter_foreign_channels(all_channel_id, all_channel_names, final_programmes, merged_sources_by_day)

    # 5) 写日志：合成后的 EPG 中每个频道每天来源
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(script_dir, "epg_source.log")
    with open(log_path, "w", encoding="utf-8") as lf:
        for cid in sorted(all_channel_id):
            ch_name = choose_channel_name(all_channel_names.get(cid, []), cid)
            day_src = merged_sources_by_day.get(cid, {})
            for day in sorted(day_src.keys()):
                srcs = sorted(day_src[day])
                src_text = ", ".join(srcs)
                lf.write(f"频道: [{ch_name}] | 日期: {day.strftime('%Y-%m-%d')}| 来源: {src_text}\n")

    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names, final_programmes, "output/epg.xml")
    compress_to_gz("output/epg.xml", "output/epg.gz")


if __name__ == "__main__":
    asyncio.run(main())
