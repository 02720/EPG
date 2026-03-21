import asyncio
import gzip
import os
import re
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import aiohttp
from opencc import OpenCC
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
CONFIG_PATH = os.path.join(BASE_DIR, "config.txt")
ALIAS_PATH = os.path.join(BASE_DIR, "alias.txt")
LOG_PATH = os.path.join(BASE_DIR, "epg_source.log")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
XML_PATH = os.path.join(OUTPUT_DIR, "epg.xml")
GZ_PATH = os.path.join(OUTPUT_DIR, "epg.gz")

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/113.0.0.0 Safari/537.36"
)
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=15, sock_connect=15, sock_read=45)

SPACE_RE = re.compile(r"\s+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r'(https?://[^\s<>"\']+|ftp://[^\s<>"\']+|www\.[^\s<>"\']+)', re.I)
DOMAIN_RE = re.compile(
    r'(?<!@)\b(?:[A-Z0-9-]+\.)+'
    r'(?:COM|CN|NET|ORG|XYZ|TV|CC|TOP|INFO|VIP|LIVE|ME|IO|CO|GG)\b'
    r'(?:/[^\s<>"\']*)?',
    re.I
)

CC = OpenCC("t2s")


@lru_cache(maxsize=100000)
def transform2_zh_hans(text: str) -> str:
    if not text:
        return ""
    return CC.convert(text)


def is_zh_lang(lang) -> bool:
    if lang is None:
        return True
    lang = str(lang).strip().lower()
    return (not lang) or lang.startswith("zh")


def strip_urls(text: str) -> str:
    if not text:
        return ""
    text = URL_RE.sub(" ", text)
    text = DOMAIN_RE.sub(" ", text)
    text = SPACE_RE.sub(" ", text)
    text = text.strip(" \t\r\n-_|/:：；;,，。.!！?？·•")
    return text


def process_display_name(display_name: str) -> str:
    if not display_name:
        return ""
    display_name = display_name.strip()
    # 去掉“高清”，但保留“超高清”
    display_name = re.sub(r"(?<!超)高清", "", display_name)
    display_name = SPACE_RE.sub(" ", display_name).strip()
    return display_name


def clean_plain_text(text: str, lang=None, remove_hd=False) -> str:
    if not text:
        return ""
    text = text.strip()
    if is_zh_lang(lang):
        text = transform2_zh_hans(text)
    text = HTML_TAG_RE.sub(" ", text)
    text = strip_urls(text)
    if remove_hd:
        text = process_display_name(text)
    text = SPACE_RE.sub(" ", text).strip()
    return text


def clean_channel_name(text: str, lang=None) -> str:
    return clean_plain_text(text, lang=lang, remove_hd=True)


def clean_title_text(text: str, lang=None) -> str:
    return clean_plain_text(text, lang=lang, remove_hd=False)


@lru_cache(maxsize=200000)
def normalize_channel_name(name: str) -> str:
    name = clean_channel_name(name, lang="zh")
    name = name.upper()
    # 注意：不移除 + 和 K，避免 CCTV5+ / 4K 之类误伤
    name = re.sub(r"[ \t\r\n\-_·•・.．,:：;；/\\|()\[\]{}【】<>《》]+", "", name)
    return name


def normalize_source_ref(value: str) -> str:
    if not value:
        return ""
    value = transform2_zh_hans(value.strip())
    value = SPACE_RE.sub(" ", value)
    return value


def format_xmltv_datetime(dt: datetime) -> str:
    return dt.astimezone(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")


def parse_xmltv_datetime(value: str):
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None

    if raw.endswith("Z"):
        raw = raw[:-1] + " +0000"

    raw = SPACE_RE.sub(" ", raw)

    m = re.match(r"^(\d{8}|\d{10}|\d{12}|\d{14})(?:\s*([+-]\d{2}:?\d{2}))?$", raw)
    if not m:
        return None

    digits, offset = m.groups()
    fmt_map = {
        8: "%Y%m%d",
        10: "%Y%m%d%H",
        12: "%Y%m%d%H%M",
        14: "%Y%m%d%H%M%S",
    }
    fmt = fmt_map.get(len(digits))
    if not fmt:
        return None

    try:
        if offset:
            offset = offset.replace(":", "")
            dt = datetime.strptime(digits + offset, fmt + "%z")
        else:
            # 无时区时默认按 UTC+8
            dt = datetime.strptime(digits, fmt).replace(tzinfo=TZ_UTC_PLUS_8)
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None


def choose_best_display_name(display_names, channel_id=""):
    for name in display_names:
        cleaned = clean_channel_name(name, lang="zh")
        if cleaned and not cleaned.isdigit():
            return cleaned
    channel_id = clean_channel_name(channel_id, lang="zh")
    if channel_id and not channel_id.isdigit():
        return channel_id
    return channel_id or ""


def load_alias_rules():
    exact_map = {}
    regex_rules = []

    if not os.path.exists(ALIAS_PATH):
        return {"exact": exact_map, "regex": regex_rules}

    with open(ALIAS_PATH, "r", encoding="utf-8-sig") as f:
        for line_no, raw_line in enumerate(f, 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            parts = [x.strip() for x in line.split(",") if x.strip()]
            if not parts:
                continue

            main_name = clean_channel_name(parts[0], lang="zh")
            if not main_name:
                continue

            exact_map[normalize_channel_name(main_name)] = main_name

            for alias in parts[1:]:
                if alias.lower().startswith("re:"):
                    pattern_text = alias[3:].strip()
                    if not pattern_text:
                        continue
                    try:
                        regex_rules.append((main_name, re.compile(pattern_text)))
                    except re.error as e:
                        print(f"alias.txt 第 {line_no} 行正则无效：{pattern_text} -> {e}")
                else:
                    alias_name = clean_channel_name(alias, lang="zh")
                    if alias_name:
                        exact_map[normalize_channel_name(alias_name)] = main_name

    return {"exact": exact_map, "regex": regex_rules}


def resolve_master_name(channel_id_clean, display_name_texts, alias_rules):
    candidates = []

    for name in display_name_texts:
        if name and name not in candidates:
            candidates.append(name)

    if channel_id_clean and not channel_id_clean.isdigit() and channel_id_clean not in candidates:
        candidates.append(channel_id_clean)

    # 1. 精确别名匹配（规范化后）
    for name in candidates:
        main_name = alias_rules["exact"].get(normalize_channel_name(name))
        if main_name:
            return main_name

    # 2. 正则别名匹配
    for name in candidates:
        for main_name, pattern in alias_rules["regex"]:
            try:
                if pattern.search(name) or pattern.search(normalize_channel_name(name)):
                    return main_name
            except re.error:
                continue

    # 3. 回退：使用当前频道最合适的显示名
    return choose_best_display_name(display_name_texts, channel_id_clean) or channel_id_clean or ""


def new_channel_entry(channel_key):
    return {
        "primary_name": channel_key,
        "display_names": [],
        "display_name_set": set(),
        "days": {}
    }


def add_display_names(entry, display_names):
    for name, lang in display_names:
        lang = lang or "zh"
        name = clean_channel_name(name, lang=lang)
        if not name:
            continue
        key = (name, lang)
        if key in entry["display_name_set"]:
            continue
        entry["display_name_set"].add(key)
        entry["display_names"].append((name, lang))


def extract_titles(programme):
    title_nodes = programme.findall("title")
    if not title_nodes:
        default_title = "精彩节目"
        return [(default_title, "zh")], len(default_title)

    titles = []
    title_keys = set()
    unique_texts = set()
    raw_nonempty_found = False

    for title in title_nodes:
        lang = title.get("lang") or "zh"
        raw_text = (title.text or "").strip()
        if raw_text:
            raw_nonempty_found = True

        cleaned_text = clean_title_text(raw_text, lang=lang)
        if not cleaned_text:
            continue

        key = (cleaned_text, lang)
        if key in title_keys:
            continue

        title_keys.add(key)
        titles.append((cleaned_text, lang))
        unique_texts.add(cleaned_text)

    # 若原本有 title，但清洗后全空，通常是被插入的网址/垃圾内容，直接丢弃
    if not titles:
        if raw_nonempty_found:
            return [], 0
        default_title = "精彩节目"
        return [(default_title, "zh")], len(default_title)

    score = sum(len(x) for x in unique_texts)
    return titles, score


def build_programme_signature(start_dt, stop_dt, titles):
    return (
        start_dt,
        stop_dt,
        tuple(text for text, _ in titles)
    )


def build_programme_element(start_dt, stop_dt, titles):
    attrib = {"start": format_xmltv_datetime(start_dt)}
    if stop_dt:
        attrib["stop"] = format_xmltv_datetime(stop_dt)

    elem = ET.Element("programme", attrib=attrib)

    for text, lang in titles:
        t = ET.SubElement(elem, "title")
        if lang:
            t.set("lang", lang)
        t.text = text

    return elem


def parse_epg(epg_content, source_url, alias_rules):
    try:
        root = ET.fromstring(epg_content)
    except ET.ParseError as e:
        print(f"[{source_url}] XML 解析失败: {e}")
        print(epg_content[:300])
        return {}

    source_ref_map = {}
    source_data = {}

    # 先解析 channel
    for channel in root.findall("channel"):
        source_ref = normalize_source_ref(channel.get("id", ""))
        channel_id_clean = clean_channel_name(channel.get("id", ""), lang="zh")

        display_pairs = []
        display_pair_set = set()

        for name_node in channel.findall("display-name"):
            lang = name_node.get("lang") or "zh"
            name = clean_channel_name(name_node.text or "", lang=lang)
            if not name:
                continue
            pair = (name, lang)
            if pair not in display_pair_set:
                display_pair_set.add(pair)
                display_pairs.append(pair)

        if channel_id_clean and not channel_id_clean.isdigit():
            pair = (channel_id_clean, "zh")
            if pair not in display_pair_set:
                display_pair_set.add(pair)
                display_pairs.append(pair)

        display_texts = [name for name, _ in display_pairs]
        canonical_name = resolve_master_name(channel_id_clean, display_texts, alias_rules)
        if not canonical_name:
            canonical_name = choose_best_display_name(display_texts, channel_id_clean) or source_ref

        meta = {
            "canonical": canonical_name,
            "display_names": display_pairs,
            "primary_name": canonical_name,
        }
        source_ref_map[source_ref] = meta

        entry = source_data.setdefault(canonical_name, new_channel_entry(canonical_name))
        add_display_names(entry, [(canonical_name, "zh")])
        add_display_names(entry, display_pairs)

    # 再解析 programme
    for programme in root.findall("programme"):
        source_ref = normalize_source_ref(programme.get("channel", ""))
        meta = source_ref_map.get(source_ref)

        if meta is None:
            fallback_id = clean_channel_name(programme.get("channel", ""), lang="zh")
            canonical_name = resolve_master_name(
                fallback_id,
                [fallback_id] if fallback_id else [],
                alias_rules
            ) or fallback_id or source_ref

            meta = {
                "canonical": canonical_name,
                "display_names": [(canonical_name, "zh")] if canonical_name else [],
                "primary_name": canonical_name,
            }
            source_ref_map[source_ref] = meta

        canonical_name = meta["canonical"]
        if not canonical_name:
            continue

        start_dt = parse_xmltv_datetime(programme.get("start", ""))
        if not start_dt:
            continue

        stop_dt = parse_xmltv_datetime(programme.get("stop", ""))
        if stop_dt and stop_dt <= start_dt:
            stop_dt = None

        titles, title_score = extract_titles(programme)
        if not titles:
            continue

        prog_elem = build_programme_element(start_dt, stop_dt, titles)

        entry = source_data.setdefault(canonical_name, new_channel_entry(canonical_name))
        add_display_names(entry, [(canonical_name, "zh")])
        add_display_names(entry, meta["display_names"])

        date_key = start_dt.date()
        day_bucket = entry["days"].setdefault(date_key, {
            "programmes": [],
            "score": 0,
            "seen": set()
        })

        sig = build_programme_signature(start_dt, stop_dt, titles)
        if sig in day_bucket["seen"]:
            continue

        day_bucket["seen"].add(sig)
        day_bucket["programmes"].append({
            "start_dt": start_dt,
            "element": prog_elem,
        })
        day_bucket["score"] += title_score

    # 清理空频道，并排序
    result = {}
    for canonical_name, entry in source_data.items():
        cleaned_days = {}
        for day, day_info in entry["days"].items():
            if not day_info["programmes"]:
                continue
            day_info["programmes"].sort(
                key=lambda x: (x["start_dt"], x["element"].attrib.get("stop", ""))
            )
            cleaned_days[day] = {
                "programmes": day_info["programmes"],
                "score": day_info["score"]
            }

        if not cleaned_days:
            continue

        entry["days"] = cleaned_days
        entry["primary_name"] = choose_best_display_name(
            [name for name, _ in entry["display_names"]],
            canonical_name
        ) or canonical_name
        result[canonical_name] = entry

    return result


def should_replace_day(current_day, candidate_day):
    if current_day is None:
        return True

    if candidate_day["score"] != current_day["score"]:
        return candidate_day["score"] > current_day["score"]

    # 同分时用节目数作次级比较
    cand_count = len(candidate_day["programmes"])
    curr_count = len(current_day["programmes"])
    if cand_count != curr_count:
        return cand_count > curr_count

    # 再比较 stop 完整度
    cand_stop_count = sum(1 for x in candidate_day["programmes"] if "stop" in x["element"].attrib)
    curr_stop_count = sum(1 for x in current_day["programmes"] if "stop" in x["element"].attrib)
    return cand_stop_count > curr_stop_count


def find_existing_channel_key(canonical_name, display_names, normalized_name_map):
    candidates = [canonical_name] + [name for name, _ in display_names]
    for name in candidates:
        norm = normalize_channel_name(name)
        if not norm or norm.isdigit():
            continue
        if norm in normalized_name_map:
            return normalized_name_map[norm]
    return None


def update_normalized_name_map(normalized_name_map, channel_key, display_names):
    names = [channel_key] + [name for name, _ in display_names]
    for name in names:
        norm = normalize_channel_name(name)
        if not norm or norm.isdigit():
            continue
        normalized_name_map.setdefault(norm, channel_key)


def merge_source_data(merged_channels, normalized_name_map, source_data, source_url):
    for canonical_name, source_entry in source_data.items():
        if not source_entry.get("days"):
            continue

        if canonical_name in merged_channels:
            merge_key = canonical_name
        else:
            merge_key = find_existing_channel_key(
                canonical_name,
                source_entry["display_names"],
                normalized_name_map
            ) or canonical_name

        if not merge_key:
            continue

        target = merged_channels.setdefault(merge_key, new_channel_entry(merge_key))
        target["primary_name"] = merge_key

        add_display_names(target, [(merge_key, "zh")])
        add_display_names(target, source_entry["display_names"])
        update_normalized_name_map(normalized_name_map, merge_key, target["display_names"])

        for day, candidate_day in source_entry["days"].items():
            candidate = {
                "programmes": candidate_day["programmes"],
                "score": candidate_day["score"],
                "source": source_url
            }

            current = target["days"].get(day)
            if should_replace_day(current, candidate):
                target["days"][day] = candidate


def get_final_display_names(channel_id, entry):
    final_names = []
    seen = set()

    preferred = [
        (entry.get("primary_name") or channel_id, "zh"),
        (channel_id, "zh"),
    ]

    for name, lang in preferred + entry["display_names"]:
        lang = lang or "zh"
        name = clean_channel_name(name, lang=lang)
        if not name:
            continue
        key = (name, lang)
        if key in seen:
            continue
        seen.add(key)
        final_names.append((name, lang))

    return final_names


def write_to_xml(merged_channels, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element("tv", attrib={"date": current_time})

    channel_order = sorted(merged_channels.keys(), key=lambda x: normalize_channel_name(x) or x)

    # 先写 channel
    for channel_id in channel_order:
        entry = merged_channels[channel_id]
        if not entry.get("days"):
            continue

        channel_elem = ET.SubElement(root, "channel", attrib={"id": channel_id})
        for display_name, lang in get_final_display_names(channel_id, entry):
            attrs = {"lang": lang} if lang else {}
            display_name_elem = ET.SubElement(channel_elem, "display-name", attrib=attrs)
            display_name_elem.text = display_name

    # 再写 programme
    for channel_id in channel_order:
        entry = merged_channels[channel_id]
        if not entry.get("days"):
            continue

        for day in sorted(entry["days"].keys()):
            for prog in entry["days"][day]["programmes"]:
                elem = prog["element"]
                elem.set("channel", channel_id)
                root.append(elem)

    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)


def write_source_log(merged_channels, filename=LOG_PATH):
    with open(filename, "w", encoding="utf-8") as f:
        for channel_id in sorted(merged_channels.keys(), key=lambda x: normalize_channel_name(x) or x):
            entry = merged_channels[channel_id]
            channel_name = entry.get("primary_name") or channel_id

            for day in sorted(entry.get("days", {}).keys()):
                source_url = entry["days"][day].get("source", "")
                f.write(
                    f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')} | 来源: {source_url}\n"
                )


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, "rb") as f_in, gzip.open(output_filename, "wb", compresslevel=5) as f_out:
        shutil.copyfileobj(f_in, f_out)


def get_urls():
    if not os.path.exists(CONFIG_PATH):
        print(f"未找到配置文件：{CONFIG_PATH}")
        return []

    urls = []
    with open(CONFIG_PATH, "r", encoding="utf-8-sig") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


async def fetch_epg(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.read()
            if not data:
                return None

            # 有些源是 .gz，有些虽然不是 .gz 后缀，但内容本身仍是 gzip
            if url.lower().endswith(".gz") or data[:2] == b"\x1f\x8b":
                try:
                    data = gzip.decompress(data)
                except OSError:
                    pass

            return data.decode("utf-8-sig", errors="ignore")

    except aiohttp.ClientResponseError as e:
        print(f"{url} HTTP错误: {e.status} {e.message}")
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")

    return None


async def main():
    urls = get_urls()
    if not urls:
        print("config.txt 中没有可用的 EPG 源。")
        return

    alias_rules = load_alias_rules()
    merged_channels = {}
    normalized_name_map = {}

    connector = aiohttp.TCPConnector(
        limit=min(32, max(8, len(urls))),
        ssl=False,
        ttl_dns_cache=300
    )
    headers = {"User-Agent": USER_AGENT}

    print("Fetching EPG data...")
    async with aiohttp.ClientSession(
        connector=connector,
        trust_env=True,
        headers=headers,
        timeout=HTTP_TIMEOUT
    ) as session:
        tasks = [fetch_epg(session, url) for url in urls]
        epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    print("Parsing and merging EPG...")
    for url, epg_content in tqdm(list(zip(urls, epg_contents)), total=len(urls), desc="Processing Sources", unit="source"):
        if not epg_content:
            continue

        source_data = parse_epg(epg_content, url, alias_rules)
        if not source_data:
            continue

        merge_source_data(merged_channels, normalized_name_map, source_data, url)

    if not merged_channels:
        print("没有可输出的 EPG 数据。")
        # 仍然生成空日志
        with open(LOG_PATH, "w", encoding="utf-8"):
            pass
        return

    print("Writing XML...")
    write_to_xml(merged_channels, XML_PATH)
    compress_to_gz(XML_PATH, GZ_PATH)
    write_source_log(merged_channels, LOG_PATH)

    print(f"完成：")
    print(f"XML: {XML_PATH}")
    print(f"GZ : {GZ_PATH}")
    print(f"LOG: {LOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
