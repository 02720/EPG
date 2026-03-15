import asyncio
import gzip
import os
import re
import shutil
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import aiohttp
from opencc import OpenCC
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CONFIG_FILE = os.path.join(BASE_DIR, "config.txt")
ALIAS_FILE = os.path.join(BASE_DIR, "alias.txt")
SOURCE_LOG_FILE = os.path.join(BASE_DIR, "epg_source.log")

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

URL_RE = re.compile(r'(?i)\b(?:https?://|www\.)[^\s<>"\']+')
XML_ENCODING_RE = re.compile(br'<\?xml[^>]*encoding=["\']([^"\']+)["\']', re.I)
SPACE_RE = re.compile(r'\s+')

_opencc_local = threading.local()


def get_opencc():
    cc = getattr(_opencc_local, "cc", None)
    if cc is None:
        cc = OpenCC("t2s")
        _opencc_local.cc = cc
    return cc


@lru_cache(maxsize=50000)
def transform2_zh_hans(text):
    if not text:
        return ""
    return get_opencc().convert(text)


@lru_cache(maxsize=50000)
def process_display_name(display_name):
    if not display_name:
        return ""
    display_name = display_name.strip()
    # 去掉“高清”，但保留“超高清”
    # 只去掉末尾或分隔场景中的“高清”，避免误伤“高清电影”这类真实名称
    display_name = re.sub(r'(?<!超)高清(?=$|[\s\-_()（）【】\[\]])', '', display_name)
    display_name = SPACE_RE.sub(" ", display_name).strip()
    return display_name


def is_zh_lang(lang):
    return lang is None or str(lang).lower().startswith("zh")


def clean_programme_text(text, lang=None):
    if not text:
        return ""
    text = text.strip()
    if not text:
        return ""
    if is_zh_lang(lang):
        text = transform2_zh_hans(text)
    # 屏蔽节目中的网址
    text = URL_RE.sub("", text)
    text = SPACE_RE.sub(" ", text).strip(" \t\r\n-—|,，;；")
    return text.strip()


@lru_cache(maxsize=50000)
def normalize_channel_name(name):
    if not name:
        return ""
    name = transform2_zh_hans(name.strip())
    name = process_display_name(name)
    name = name.replace("＋", "+").replace("中央电视台", "CCTV")
    name = re.sub(r'(频道|頻道)$', "", name)
    name = re.sub(r'[ \t\r\n\-_/]+', "", name)
    name = re.sub(r'[()（）\[\]【】]', "", name)
    name = name.upper()
    # CCTV01 -> CCTV1
    name = re.sub(r'^CCTV0+([1-9]\d*)(?![0-9K\+])', r'CCTV\1', name)
    return name


def parse_xmltv_datetime(value):
    """
    兼容以下格式：
    1. 20260301000600 +0800
    2. 20260301000600+0800
    3. 20260301000600
    4. 202603010006
    5. 20260301000600Z
    """
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    if s.endswith("Z"):
        s = s[:-1] + "+0000"

    s = re.sub(r'\s+', "", s)
    s = re.sub(r'([+-]\d{2}):?(\d{2})$', r'\1\2', s)

    if re.search(r'[+-]\d{2}$', s):
        s += "00"

    formats = (
        "%Y%m%d%H%M%S%z",
        "%Y%m%d%H%M%z",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
    )

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)
            return dt.astimezone(TZ_UTC_PLUS_8)
        except ValueError:
            continue

    return None


def format_xmltv_datetime(dt):
    return dt.astimezone(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")


def decode_xml_bytes(data):
    if not data:
        return ""

    encodings = []
    match = XML_ENCODING_RE.search(data[:200])
    if match:
        try:
            encodings.append(match.group(1).decode("ascii", errors="ignore"))
        except Exception:
            pass

    encodings.extend(["utf-8", "utf-8-sig", "gb18030"])

    tried = set()
    for enc in encodings:
        enc_l = enc.lower()
        if enc_l in tried:
            continue
        tried.add(enc_l)
        try:
            return data.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue

    return data.decode("utf-8", errors="ignore")


def dedupe_name_nodes(nodes):
    seen = set()
    result = []
    for name, lang in nodes:
        name = (name or "").strip()
        lang = lang or "zh"
        if not name:
            continue
        key = (name, lang)
        if key not in seen:
            seen.add(key)
            result.append((name, lang))
    return result


def dedupe_text_nodes(nodes):
    seen = set()
    result = []
    for lang, text in nodes:
        lang = lang or "zh"
        text = (text or "").strip()
        if not text:
            continue
        key = (lang, text)
        if key not in seen:
            seen.add(key)
            result.append((lang, text))
    return result


def new_channel_entry(main_name):
    main_name = main_name or "UNKNOWN"
    entry = {
        "main_name": main_name,
        "display_names": [],
        "display_name_keys": set(),
        "days": {},
    }
    add_display_names(entry, [(main_name, "zh")])
    return entry


def add_display_names(entry, display_names):
    for name, lang in display_names:
        name = (name or "").strip()
        lang = lang or "zh"
        if not name:
            continue
        key = (name, lang)
        if key not in entry["display_name_keys"]:
            entry["display_name_keys"].add(key)
            entry["display_names"].append((name, lang))


def maybe_update_main_name(entry, new_name):
    new_name = (new_name or "").strip()
    if not new_name:
        return

    current = entry.get("main_name", "")
    if not current or current == "UNKNOWN":
        entry["main_name"] = new_name
        add_display_names(entry, [(new_name, "zh")])
        return

    cur_norm = normalize_channel_name(current)
    new_norm = normalize_channel_name(new_name)
    if new_norm and cur_norm == new_norm and len(new_name) < len(current):
        entry["main_name"] = new_name
        add_display_names(entry, [(new_name, "zh")])


def finalize_day_info(day_info):
    programmes = sorted(
        day_info["programmes"],
        key=lambda p: (p["start"], p["stop"], tuple(text for _, text in p["titles"])),
    )

    seen = set()
    deduped = []
    for prog in programmes:
        sig = (
            prog["start"],
            prog["stop"],
            tuple(text for _, text in prog["titles"]),
        )
        if sig in seen:
            continue
        seen.add(sig)
        deduped.append(prog)

    day_info["programmes"] = deduped
    day_info["count"] = len(deduped)
    day_info["duration"] = sum(int((p["stop"] - p["start"]).total_seconds()) for p in deduped)
    day_info["score"] = sum(len(text) for p in deduped for _, text in p["titles"])


def should_replace_day(candidate, existing):
    if existing is None:
        return True
    # 主排序：title总长度
    # 次排序：节目数、时长，仅在title长度相同的情况下作为兜底
    return (
        candidate.get("score", 0),
        candidate.get("count", 0),
        candidate.get("duration", 0),
    ) > (
        existing.get("score", 0),
        existing.get("count", 0),
        existing.get("duration", 0),
    )


class AliasResolver:
    def __init__(self, exact_map=None, regex_rules=None):
        self.exact_map = exact_map or {}
        self.regex_rules = regex_rules or []

    def resolve(self, names):
        prepared = []
        for name in names:
            name = process_display_name(transform2_zh_hans((name or "").strip()))
            if name:
                prepared.append(name)

        # 精确别名优先
        for name in prepared:
            main_name = self.exact_map.get(normalize_channel_name(name))
            if main_name:
                return main_name

        # 正则别名其次
        for main_name, pattern in self.regex_rules:
            for name in prepared:
                if pattern.search(name):
                    return main_name

        return None


def load_aliases(alias_file=ALIAS_FILE):
    exact_map = {}
    regex_rules = []

    if not os.path.exists(alias_file):
        return AliasResolver(exact_map, regex_rules)

    with open(alias_file, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = [part.strip() for part in line.split(",") if part.strip()]
            if not parts:
                continue

            main_name = process_display_name(transform2_zh_hans(parts[0]))
            if not main_name:
                continue

            exact_map[normalize_channel_name(main_name)] = main_name

            for alias in parts[1:]:
                if alias.startswith("re:"):
                    pattern_text = alias[3:]
                    try:
                        regex_rules.append((main_name, re.compile(pattern_text)))
                    except re.error as e:
                        print(f"[WARN] alias.txt 第 {line_no} 行正则无效：{pattern_text} | {e}")
                else:
                    alias_name = process_display_name(transform2_zh_hans(alias))
                    if alias_name:
                        exact_map[normalize_channel_name(alias_name)] = main_name

    return AliasResolver(exact_map, regex_rules)


def choose_best_name(names):
    prepared = []
    for name in names:
        name = process_display_name(transform2_zh_hans((name or "").strip()))
        if name:
            prepared.append(name)

    for name in prepared:
        if not name.isdigit():
            return name
    return prepared[0] if prepared else "UNKNOWN"


def resolve_main_name(candidates, alias_resolver):
    return alias_resolver.resolve(candidates) or choose_best_name(candidates)


async def fetch_epg(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()

            # 兼容 .gz 文件
            if data[:2] == b"\x1f\x8b":
                try:
                    data = gzip.decompress(data)
                except OSError:
                    pass

            return decode_xml_bytes(data)
    except aiohttp.ClientError as e:
        print(f"[ERROR] {url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"[ERROR] {url} 请求超时")
    except Exception as e:
        print(f"[ERROR] {url} 其他错误: {e}")
    return None


def parse_epg(epg_content, source_url, alias_resolver):
    try:
        root = ET.fromstring(epg_content)
    except ET.ParseError as e:
        print(f"[ERROR] XML 解析失败: {source_url} | {e}")
        print(f"[ERROR] 内容片段: {epg_content[:300]}")
        return None

    today = datetime.now(TZ_UTC_PLUS_8).date()

    # 每个源内部先按原始频道存储，再折叠到 canonical channel
    source_channels = {}
    channel_lookup = {}
    channel_lookup_norm = {}

    # 先解析 channel
    for channel in root.findall("channel"):
        raw_id = (channel.get("id") or "").strip()
        processed_id = process_display_name(transform2_zh_hans(raw_id))

        display_names = []
        for node in channel.findall("display-name"):
            raw_name = (node.text or "").strip()
            if not raw_name:
                continue
            name = process_display_name(transform2_zh_hans(raw_name))
            if not name:
                continue
            display_names.append((name, node.get("lang") or "zh"))

        if processed_id and not processed_id.isdigit():
            display_names.append((processed_id, "zh"))

        display_names = dedupe_name_nodes(display_names)

        candidates = [name for name, _ in display_names]
        if processed_id:
            candidates.append(processed_id)

        main_name = resolve_main_name(candidates, alias_resolver)
        canonical_key = normalize_channel_name(main_name or processed_id or raw_id)
        if not canonical_key:
            continue

        source_key = raw_id or processed_id or f"_CH_{len(source_channels) + 1}"

        entry = source_channels.get(source_key)
        if entry is None:
            entry = new_channel_entry(main_name or processed_id or raw_id or canonical_key)
            entry["canonical_key"] = canonical_key
            source_channels[source_key] = entry
        else:
            entry["canonical_key"] = canonical_key
            maybe_update_main_name(entry, main_name)

        add_display_names(entry, display_names)

        if raw_id:
            channel_lookup[raw_id] = source_key
        if processed_id:
            channel_lookup[processed_id] = source_key

        norm_id = normalize_channel_name(processed_id or raw_id)
        if norm_id:
            channel_lookup_norm[norm_id] = source_key

        # 额外使用 display-name 的严格归一化结果辅助匹配，降低串台概率
        for name, _ in display_names:
            norm_name = normalize_channel_name(name)
            if norm_name:
                channel_lookup_norm[norm_name] = source_key

    # 再解析 programme
    for programme in root.findall("programme"):
        start = parse_xmltv_datetime(programme.get("start"))
        stop = parse_xmltv_datetime(programme.get("stop"))

        if not start or not stop or stop <= start:
            continue
        if stop.date() < today:
            continue

        raw_channel = (programme.get("channel") or "").strip()
        processed_channel = process_display_name(transform2_zh_hans(raw_channel))
        normalized_channel = normalize_channel_name(processed_channel or raw_channel)

        source_key = (
            channel_lookup.get(raw_channel)
            or channel_lookup.get(processed_channel)
            or channel_lookup_norm.get(normalized_channel)
        )

        if source_key is None:
            inferred_name = resolve_main_name([processed_channel or raw_channel], alias_resolver)
            canonical_key = normalize_channel_name(inferred_name or processed_channel or raw_channel)
            if not canonical_key:
                continue

            source_key = processed_channel or raw_channel or f"_CH_{len(source_channels) + 1}"
            entry = new_channel_entry(inferred_name or processed_channel or raw_channel or canonical_key)
            entry["canonical_key"] = canonical_key
            source_channels[source_key] = entry

            if raw_channel:
                channel_lookup[raw_channel] = source_key
            if processed_channel:
                channel_lookup[processed_channel] = source_key
            if normalized_channel:
                channel_lookup_norm[normalized_channel] = source_key

        entry = source_channels[source_key]
        if not entry.get("canonical_key"):
            entry["canonical_key"] = normalize_channel_name(entry["main_name"])

        titles = []
        for node in programme.findall("title"):
            cleaned = clean_programme_text(node.text, node.get("lang"))
            if cleaned:
                titles.append((node.get("lang") or "zh", cleaned))
        titles = dedupe_text_nodes(titles)

        # 只保留有有效 title 的节目；这也能滤掉纯网址节目
        if not titles:
            continue

        # 按你的要求：合成时仅比较和保留 title，忽略 desc
        prog_obj = {
            "start": start,
            "stop": stop,
            "titles": titles,
        }

        day_key = start.date().isoformat()
        day_info = entry["days"].setdefault(
            day_key,
            {"programmes": [], "score": 0, "count": 0, "duration": 0},
        )
        day_info["programmes"].append(prog_obj)
        day_info["count"] += 1
        day_info["duration"] += int((stop - start).total_seconds())
        day_info["score"] += sum(len(text) for _, text in titles)

    # 单个源内部先按 canonical channel 折叠
    # 同一天如果出现多个同主名频道，仍按 title 总长度保留更丰富的一组
    collapsed = {}
    for entry in source_channels.values():
        for day_key in list(entry["days"].keys()):
            finalize_day_info(entry["days"][day_key])
            if not entry["days"][day_key]["programmes"]:
                del entry["days"][day_key]

        if not entry["days"]:
            continue

        canonical_key = entry.get("canonical_key") or normalize_channel_name(entry["main_name"])
        if not canonical_key:
            continue

        dst = collapsed.get(canonical_key)
        if dst is None:
            dst = new_channel_entry(entry["main_name"])
            dst["canonical_key"] = canonical_key
            collapsed[canonical_key] = dst

        maybe_update_main_name(dst, entry["main_name"])
        add_display_names(dst, entry["display_names"])

        for day_key, day_info in entry["days"].items():
            candidate = {
                "programmes": day_info["programmes"],
                "score": day_info["score"],
                "count": day_info["count"],
                "duration": day_info["duration"],
                "source": source_url,
            }
            existing = dst["days"].get(day_key)
            if should_replace_day(candidate, existing):
                dst["days"][day_key] = candidate

    return {"source_url": source_url, "channels": collapsed}


async def fetch_and_parse(session, url, alias_resolver):
    content = await fetch_epg(session, url)
    if not content:
        return None
    return await asyncio.to_thread(parse_epg, content, url, alias_resolver)


def merge_channel_collections(target, source_channels):
    for canonical_key, src in source_channels.items():
        dst = target.get(canonical_key)
        if dst is None:
            dst = new_channel_entry(src["main_name"])
            dst["canonical_key"] = canonical_key
            target[canonical_key] = dst

        maybe_update_main_name(dst, src["main_name"])
        add_display_names(dst, src["display_names"])

        for day_key, day_info in src["days"].items():
            existing = dst["days"].get(day_key)
            if should_replace_day(day_info, existing):
                dst["days"][day_key] = day_info


def build_output_channel_ids(merged_channels):
    used = set()
    mapping = {}
    ordered_keys = sorted(merged_channels, key=lambda k: merged_channels[k]["main_name"])

    for key in ordered_keys:
        preferred = merged_channels[key]["main_name"] or key
        channel_id = preferred
        index = 2
        while channel_id in used:
            channel_id = f"{preferred}_{index}"
            index += 1
        used.add(channel_id)
        mapping[key] = channel_id

    return mapping


def write_to_xml(merged_channels, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    root = ET.Element("tv", attrib={"date": format_xmltv_datetime(datetime.now(TZ_UTC_PLUS_8))})
    channel_id_map = build_output_channel_ids(merged_channels)

    ordered_keys = sorted(merged_channels, key=lambda k: merged_channels[k]["main_name"])

    for key in ordered_keys:
        entry = merged_channels[key]
        channel_id = channel_id_map[key]

        channel_elem = ET.SubElement(root, "channel", attrib={"id": channel_id})

        display_names = entry["display_names"][:]
        display_names.sort(key=lambda item: (0 if item[0] == entry["main_name"] else 1, item[0], item[1]))

        for display_name, lang in display_names:
            attrs = {"lang": lang} if lang else {}
            display_name_elem = ET.SubElement(channel_elem, "display-name", attrib=attrs)
            display_name_elem.text = display_name

        for day_key in sorted(entry["days"]):
            for prog in entry["days"][day_key]["programmes"]:
                prog_elem = ET.SubElement(
                    root,
                    "programme",
                    attrib={
                        "channel": channel_id,
                        "start": format_xmltv_datetime(prog["start"]),
                        "stop": format_xmltv_datetime(prog["stop"]),
                    },
                )

                for lang, text in prog["titles"]:
                    attrs = {"lang": lang} if lang else {}
                    title_elem = ET.SubElement(prog_elem, "title", attrib=attrs)
                    title_elem.text = text

    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)
    return channel_id_map


def write_source_log(merged_channels, channel_id_map, filename=SOURCE_LOG_FILE):
    ordered_keys = sorted(merged_channels, key=lambda k: merged_channels[k]["main_name"])
    with open(filename, "w", encoding="utf-8") as f:
        for key in ordered_keys:
            entry = merged_channels[key]
            channel_id = channel_id_map[key]
            display_name = entry["main_name"] or (
                entry["display_names"][0][0] if entry["display_names"] else channel_id
            )

            for day_key in sorted(entry["days"]):
                source = entry["days"][day_key].get("source", "")
                f.write(f"频道: [{channel_id}（{display_name}）] | 日期: {day_key} | 来源: {source}\n")


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, "rb") as f_in, gzip.open(output_filename, "wb", compresslevel=5) as f_out:
        shutil.copyfileobj(f_in, f_out)


def get_urls(config_file=CONFIG_FILE):
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"未找到配置文件: {config_file}")

    urls = []
    with open(config_file, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


async def main():
    urls = get_urls()
    if not urls:
        print("config.txt 中没有可用的 EPG URL")
        return

    alias_resolver = load_aliases()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
    }
    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_read=60)
    connector = aiohttp.TCPConnector(limit=16, ssl=False, ttl_dns_cache=300)

    print("正在获取并解析 EPG...")
    async with aiohttp.ClientSession(
        connector=connector,
        trust_env=True,
        headers=headers,
        timeout=timeout,
    ) as session:
        tasks = [fetch_and_parse(session, url, alias_resolver) for url in urls]
        results = await tqdm_asyncio.gather(*tasks, desc="Fetch+Parse", unit="source")

    print("正在合成 EPG...")
    merged_channels = {}
    with tqdm(total=len(results), desc="Merging", unit="source") as pbar:
        for result in results:
            if result and result.get("channels"):
                merge_channel_collections(merged_channels, result["channels"])
            pbar.update(1)

    xml_file = os.path.join(OUTPUT_DIR, "epg.xml")
    gz_file = os.path.join(OUTPUT_DIR, "epg.gz")

    print("正在写入 XML...")
    channel_id_map = write_to_xml(merged_channels, xml_file)

    print("正在写入来源日志...")
    write_source_log(merged_channels, channel_id_map, SOURCE_LOG_FILE)

    print("正在压缩 GZ...")
    compress_to_gz(xml_file, gz_file)

    print(f"完成：{xml_file}")
    print(f"完成：{gz_file}")
    print(f"完成：{SOURCE_LOG_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
