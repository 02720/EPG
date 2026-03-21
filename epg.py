import asyncio
import gzip
import io
import os
import re
import shutil
import unicodedata
import xml.etree.ElementTree as ET

from collections import defaultdict
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from urllib.parse import urlparse

import aiohttp
from opencc import OpenCC
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio


TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
CC = OpenCC("t2s")

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
CONFIG_PATH = os.path.join(BASE_DIR, "config.txt")
ALIAS_PATH = os.path.join(BASE_DIR, "alias.txt")
LOG_PATH = os.path.join(BASE_DIR, "epg_source.log")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_XML = os.path.join(OUTPUT_DIR, "epg.xml")
OUTPUT_GZ = os.path.join(OUTPUT_DIR, "epg.gz")

RE_MULTI_SPACE = re.compile(r"\s+")
RE_INVALID_XML_CTRL = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")
RE_URL = re.compile(r"(?i)\b(?:https?://|www\.)[^\s<>'\"]+")
RE_PURE_URLISH = re.compile(
    r"(?i)^\s*(?:https?://)?(?:www\.)?[a-z0-9.-]+\.[a-z]{2,}(?::\d+)?(?:[/?#][^\s]*)?\s*/?\s*$"
)
RE_MATCH_CLEAN = re.compile(r"[\s\-_·•・]+")

# 去除“高清”，但保留“超高清”
RE_HD = re.compile(r"(?<!超)高清")


def local_name(tag):
    if not isinstance(tag, str):
        return ""
    return tag.split("}", 1)[-1]


@lru_cache(maxsize=200000)
def transform2_zh_hans(text):
    if text is None:
        return ""
    return CC.convert(text)


@lru_cache(maxsize=100000)
def normalize_xml_channel_ref(raw):
    if raw is None:
        return ""
    raw = unicodedata.normalize("NFKC", str(raw))
    raw = transform2_zh_hans(raw)
    return raw.strip()


def process_display_name(display_name):
    if not display_name:
        return ""
    display_name = RE_HD.sub("", display_name)
    display_name = RE_MULTI_SPACE.sub(" ", display_name).strip()
    return display_name


@lru_cache(maxsize=100000)
def cleanup_channel_name(name):
    name = normalize_xml_channel_ref(name)
    name = process_display_name(name)
    name = RE_MULTI_SPACE.sub(" ", name).strip()
    return name


@lru_cache(maxsize=100000)
def normalize_name_for_match(name):
    name = cleanup_channel_name(name).upper()
    name = RE_MATCH_CLEAN.sub("", name)
    return name


def get_source_host(url):
    try:
        host = urlparse(url).netloc.lower()
        if "@" in host:
            host = host.split("@", 1)[1]
        if ":" in host:
            host = host.split(":", 1)[0]
        return host
    except Exception:
        return ""


def looks_like_url_or_source(text, source_host=""):
    if not text:
        return False
    t = unicodedata.normalize("NFKC", str(text)).strip().strip("/")
    if not t:
        return False

    low = t.lower()

    if RE_PURE_URLISH.match(low):
        return True

    if source_host:
        host = source_host.lower().strip("/")
        stripped = low
        if stripped.startswith("http://"):
            stripped = stripped[7:]
        elif stripped.startswith("https://"):
            stripped = stripped[8:]
        if stripped.startswith("www."):
            stripped = stripped[4:]
        stripped = stripped.strip("/")
        if stripped == host or stripped.startswith(host + "/"):
            return True

    return False


def sanitize_programme_title(text, source_host=""):
    if text is None:
        return ""

    text = unicodedata.normalize("NFKC", str(text))
    text = transform2_zh_hans(text).strip()

    if not text:
        return ""

    # 纯网址/源站主页直接丢弃
    if looks_like_url_or_source(text, source_host):
        return ""

    # 移除一般 URL
    text = RE_URL.sub("", text)

    # 额外移除与源站 host 相关的文本
    if source_host:
        text = re.sub(
            rf"(?i)\b(?:https?://)?(?:www\.)?{re.escape(source_host)}(?::\d+)?(?:[/?#][^\s<>'\"]*)?\b",
            "",
            text,
        )

    text = RE_MULTI_SPACE.sub(" ", text).strip(" \t\r\n-_|/：:;，,")

    if not text:
        return ""

    if looks_like_url_or_source(text, source_host):
        return ""

    return text


def parse_xmltv_datetime(raw):
    """
    兼容：
    - 20260301000600 +0800
    - 20260301000600+0800
    - 20260301000600
    - 202603010006
    - 20260301
    - ''
    """
    if not raw:
        return None

    s = unicodedata.normalize("NFKC", str(raw)).strip()
    if not s:
        return None

    s = RE_MULTI_SPACE.sub("", s)
    if not s:
        return None

    if s.endswith("Z"):
        s = s[:-1] + "+0000"

    m = re.match(r"^(\d{8}|\d{12}|\d{14})([+-]\d{4})?$", s)
    if not m:
        return None

    digits, offset = m.groups()
    fmt = {
        8: "%Y%m%d",
        12: "%Y%m%d%H%M",
        14: "%Y%m%d%H%M%S",
    }[len(digits)]

    try:
        dt = datetime.strptime(digits, fmt)
    except ValueError:
        return None

    if offset:
        sign = 1 if offset[0] == "+" else -1
        hours = int(offset[1:3])
        minutes = int(offset[3:5])
        tzinfo = timezone(sign * timedelta(hours=hours, minutes=minutes))
    else:
        # 无时区时，按东八区处理，避免 %z 报错
        tzinfo = TZ_UTC_PLUS_8

    return dt.replace(tzinfo=tzinfo).astimezone(TZ_UTC_PLUS_8)


def format_xmltv_datetime(dt):
    return dt.astimezone(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")


class AliasResolver:
    def __init__(self, path):
        self.path = path
        self.exact = {}
        self.regex = []
        self.main_name_keys = set()
        self.load()

    def load(self):
        if not os.path.exists(self.path):
            return

        with open(self.path, "r", encoding="utf-8") as f:
            for lineno, raw_line in enumerate(f, 1):
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue

                parts = [p.strip() for p in line.split(",") if p.strip()]
                if not parts:
                    continue

                main_name = cleanup_channel_name(parts[0])
                if not main_name:
                    continue

                main_key = normalize_name_for_match(main_name)
                self.main_name_keys.add(main_key)
                self.exact[main_key] = main_name

                for alias in parts[1:]:
                    if alias.startswith("re:"):
                        pattern_text = alias[3:].strip()
                        if not pattern_text:
                            continue
                        try:
                            self.regex.append((re.compile(pattern_text), main_name))
                        except re.error as e:
                            print(f"alias.txt 第 {lineno} 行正则无效: {alias} | {e}")
                    else:
                        alias_name = cleanup_channel_name(alias)
                        if alias_name:
                            self.exact[normalize_name_for_match(alias_name)] = main_name

    def resolve(self, name):
        clean = cleanup_channel_name(name)
        if not clean:
            return None, None

        key = normalize_name_for_match(clean)
        if key in self.exact:
            return self.exact[key], "exact"

        for pattern, main_name in self.regex:
            if pattern.search(clean):
                return main_name, "regex"

        return None, None

    def is_main_name(self, name):
        return normalize_name_for_match(name) in self.main_name_keys


def choose_primary_name(display_names, fallback=""):
    for name in display_names:
        if name and not looks_like_url_or_source(name) and not name.isdigit():
            return name

    if fallback and not looks_like_url_or_source(fallback):
        return fallback

    for name in display_names:
        if name:
            return name

    return fallback


def resolve_canonical_name(candidate_names, channel_id_name, alias_resolver):
    ordered = []
    seen = set()

    for name in list(candidate_names) + ([channel_id_name] if channel_id_name else []):
        clean = cleanup_channel_name(name)
        if not clean or clean in seen or looks_like_url_or_source(clean):
            continue
        seen.add(clean)
        ordered.append(clean)

    alias_hits = {}
    for idx, name in enumerate(ordered):
        main_name, match_type = alias_resolver.resolve(name)
        if not main_name:
            continue

        item = alias_hits.setdefault(main_name, {"priority": 0, "count": 0, "idx": idx})
        item["priority"] = max(item["priority"], 2 if match_type == "exact" else 1)
        item["count"] += 1
        item["idx"] = min(item["idx"], idx)

    if alias_hits:
        main_name = sorted(
            alias_hits.items(),
            key=lambda kv: (-kv[1]["priority"], -kv[1]["count"], kv[1]["idx"], -len(kv[0]))
        )[0][0]
        display_name = main_name
    else:
        fallback = cleanup_channel_name(channel_id_name or "")
        display_name = choose_primary_name(ordered, fallback)

    canonical_key = normalize_name_for_match(display_name) if display_name else ""
    return canonical_key, display_name, ordered


def prefer_channel_name(new_name, old_name, alias_resolver):
    if not new_name:
        return False
    if not old_name:
        return True

    new_is_main = alias_resolver.is_main_name(new_name)
    old_is_main = alias_resolver.is_main_name(old_name)

    if new_is_main != old_is_main:
        return new_is_main

    if normalize_name_for_match(new_name) == normalize_name_for_match(old_name):
        return len(new_name) < len(old_name)

    return False


def order_display_names(primary_name, name_map):
    ordered = {}
    if primary_name:
        ordered[primary_name] = name_map.get(primary_name, "zh")

    for name, lang in name_map.items():
        if name and name not in ordered:
            ordered[name] = lang or "zh"

    return ordered


def dedupe_programmes(programmes):
    programmes.sort(key=lambda p: (p["start"], p["stop"], tuple(t for t, _ in p["titles"])))
    result = []
    seen = set()

    for prog in programmes:
        key = (
            prog["start"],
            prog["stop"],
            tuple(t for t, _ in prog["titles"]),
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(prog)

    return result


def calc_day_title_score(programmes):
    total = 0
    for prog in programmes:
        for title, _lang in prog["titles"]:
            total += len(title.strip())
    return total


def is_better_day(new_day, old_day):
    if old_day is None:
        return True

    if new_day["title_score"] != old_day["title_score"]:
        return new_day["title_score"] > old_day["title_score"]

    # 仅作为同分时的次级比较，不改变主逻辑
    if new_day["programme_count"] != old_day["programme_count"]:
        return new_day["programme_count"] > old_day["programme_count"]

    return False


async def fetch_epg(session, url, sem):
    async with sem:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.read()

                # 兼容 .gz 文件与 gzip magic header
                if url.lower().endswith(".gz") or data[:2] == b"\x1f\x8b":
                    try:
                        data = gzip.decompress(data)
                    except OSError:
                        pass

                charset = response.charset or "utf-8"
                return url, data.decode(charset, errors="ignore")

        except aiohttp.ClientError as e:
            print(f"{url} HTTP请求错误: {e}")
        except asyncio.TimeoutError:
            print(f"{url} 请求超时")
        except Exception as e:
            print(f"{url} 其他错误: {e}")

    return url, None


def parse_epg(epg_content, source_url, alias_resolver):
    if not epg_content:
        return None

    epg_content = RE_INVALID_XML_CTRL.sub("", epg_content)
    source_host = get_source_host(source_url)

    channel_meta_by_ref = {}
    raw_programmes = defaultdict(lambda: defaultdict(list))
    source_channels = defaultdict(lambda: {
        "channel_name": "",
        "display_names": {},
        "match_keys": set(),
    })
    source_days = defaultdict(dict)

    try:
        for _event, elem in ET.iterparse(io.StringIO(epg_content), events=("end",)):
            tag = local_name(elem.tag)

            if tag == "channel":
                channel_ref = normalize_xml_channel_ref(elem.get("id", ""))
                channel_id_name = cleanup_channel_name(channel_ref)

                display_names = []
                seen_names = set()

                for child in elem:
                    if local_name(child.tag) != "display-name":
                        continue

                    name = cleanup_channel_name(child.text or "")
                    if not name or looks_like_url_or_source(name):
                        continue

                    if name not in seen_names:
                        seen_names.add(name)
                        display_names.append((name, child.get("lang") or "zh"))

                if channel_id_name and channel_id_name not in seen_names and not looks_like_url_or_source(channel_id_name):
                    display_names.append((channel_id_name, "zh"))

                candidate_names = [name for name, _lang in display_names]
                canonical_key, canonical_display, ordered_names = resolve_canonical_name(
                    candidate_names,
                    channel_id_name,
                    alias_resolver
                )

                if not canonical_key:
                    canonical_display = choose_primary_name(candidate_names, channel_id_name)
                    canonical_key = normalize_name_for_match(canonical_display) if canonical_display else ""

                if canonical_key:
                    channel_meta_by_ref[channel_ref] = {
                        "canonical_key": canonical_key,
                        "canonical_display": canonical_display or canonical_key,
                        "display_names": display_names,
                    }

                    bucket = source_channels[canonical_key]
                    if prefer_channel_name(canonical_display, bucket["channel_name"], alias_resolver):
                        bucket["channel_name"] = canonical_display
                    elif not bucket["channel_name"]:
                        bucket["channel_name"] = canonical_display or canonical_key

                    if bucket["channel_name"]:
                        bucket["display_names"].setdefault(bucket["channel_name"], "zh")
                        bucket["match_keys"].add(normalize_name_for_match(bucket["channel_name"]))

                    for name, lang in display_names:
                        bucket["display_names"].setdefault(name, lang or "zh")
                        mk = normalize_name_for_match(name)
                        if mk:
                            bucket["match_keys"].add(mk)

                    for name in ordered_names:
                        mk = normalize_name_for_match(name)
                        if mk:
                            bucket["match_keys"].add(mk)

                elem.clear()

            elif tag == "programme":
                channel_ref = normalize_xml_channel_ref(elem.get("channel", ""))
                if not channel_ref:
                    elem.clear()
                    continue

                meta = channel_meta_by_ref.get(channel_ref)
                if meta is None:
                    fallback_name = cleanup_channel_name(channel_ref)
                    canonical_key, canonical_display, ordered_names = resolve_canonical_name(
                        [fallback_name] if fallback_name else [],
                        fallback_name,
                        alias_resolver
                    )
                    if not canonical_key and fallback_name:
                        canonical_display = fallback_name
                        canonical_key = normalize_name_for_match(fallback_name)

                    if not canonical_key:
                        elem.clear()
                        continue

                    meta = {
                        "canonical_key": canonical_key,
                        "canonical_display": canonical_display or canonical_key,
                        "display_names": [(canonical_display or canonical_key, "zh")],
                    }
                    channel_meta_by_ref[channel_ref] = meta

                    bucket = source_channels[canonical_key]
                    if prefer_channel_name(meta["canonical_display"], bucket["channel_name"], alias_resolver):
                        bucket["channel_name"] = meta["canonical_display"]
                    elif not bucket["channel_name"]:
                        bucket["channel_name"] = meta["canonical_display"]

                    bucket["display_names"].setdefault(meta["canonical_display"], "zh")
                    bucket["match_keys"].add(canonical_key)

                start_dt = parse_xmltv_datetime(elem.get("start", ""))
                if not start_dt:
                    elem.clear()
                    continue

                stop_dt = parse_xmltv_datetime(elem.get("stop", ""))
                if stop_dt is None or stop_dt < start_dt:
                    stop_dt = start_dt

                titles = []
                title_seen = set()

                for child in elem:
                    if local_name(child.tag) != "title":
                        continue

                    title_text = sanitize_programme_title(child.text or "", source_host)
                    if not title_text:
                        continue

                    if title_text in title_seen:
                        continue
                    title_seen.add(title_text)

                    lang = child.get("lang")
                    titles.append((title_text, lang))

                # 无有效 title 的节目直接丢弃
                if not titles:
                    elem.clear()
                    continue

                day = start_dt.date()
                raw_programmes[channel_ref][day].append({
                    "start": start_dt,
                    "stop": stop_dt,
                    "titles": titles,
                })

                elem.clear()

    except ET.ParseError as e:
        print(f"解析 XML 失败: {source_url} | {e}")
        return None
    except Exception as e:
        print(f"处理 EPG 失败: {source_url} | {e}")
        return None

    # 同一来源内部，若多个 channel-id 被归并到同一主名，则仍按“同频道同天 title 总长度”择优
    for channel_ref, day_map in raw_programmes.items():
        meta = channel_meta_by_ref.get(channel_ref)
        if not meta:
            continue

        canonical_key = meta["canonical_key"]

        for day, programmes in day_map.items():
            programmes = dedupe_programmes(programmes)
            day_data = {
                "programmes": programmes,
                "title_score": calc_day_title_score(programmes),
                "programme_count": len(programmes),
            }

            old_day = source_days[canonical_key].get(day)
            if is_better_day(day_data, old_day):
                source_days[canonical_key][day] = day_data

    # 过滤无节目的频道
    filtered_channels = {}
    filtered_days = {}

    for canonical_key, day_map in source_days.items():
        if not day_map:
            continue

        meta = source_channels.get(canonical_key)
        if not meta:
            continue

        channel_name = meta["channel_name"] or next(iter(meta["display_names"]), canonical_key)
        meta["channel_name"] = channel_name
        meta["display_names"] = order_display_names(channel_name, meta["display_names"])
        filtered_channels[canonical_key] = meta
        filtered_days[canonical_key] = day_map

    return {
        "source_url": source_url,
        "channels": filtered_channels,
        "days": filtered_days,
    }


def merge_source_into_global(parsed, merged_channels, merged_days, global_match_map, conflict_match_keys, alias_resolver):
    if not parsed:
        return

    def register_match_keys(target_key, match_keys):
        for mk in match_keys:
            if not mk:
                continue
            if mk in conflict_match_keys:
                continue

            prev = global_match_map.get(mk)
            if prev is None:
                global_match_map[mk] = target_key
            elif prev != target_key:
                conflict_match_keys.add(mk)
                global_match_map.pop(mk, None)

    source_key_map = {}

    # 先合并频道元信息
    for src_key, meta in parsed["channels"].items():
        candidate_targets = set()

        for mk in set(meta["match_keys"]) | {src_key}:
            target = global_match_map.get(mk)
            if target:
                candidate_targets.add(target)

        if src_key in merged_channels:
            target_key = src_key
        elif len(candidate_targets) == 1:
            # 只有唯一匹配目标时才自动合并，尽量避免串台
            target_key = next(iter(candidate_targets))
        else:
            target_key = src_key

        source_key_map[src_key] = target_key

        bucket = merged_channels.setdefault(target_key, {
            "channel_name": "",
            "display_names": {},
            "match_keys": set(),
        })

        if prefer_channel_name(meta["channel_name"], bucket["channel_name"], alias_resolver):
            bucket["channel_name"] = meta["channel_name"]
        elif not bucket["channel_name"]:
            bucket["channel_name"] = meta["channel_name"] or target_key

        for name, lang in meta["display_names"].items():
            if name:
                bucket["display_names"].setdefault(name, lang or "zh")

        bucket["match_keys"].update(meta["match_keys"])
        if bucket["channel_name"]:
            bucket["display_names"].setdefault(bucket["channel_name"], "zh")
            bucket["match_keys"].add(normalize_name_for_match(bucket["channel_name"]))

        register_match_keys(target_key, bucket["match_keys"])

    # 再按“频道-天”合并节目
    for src_key, day_map in parsed["days"].items():
        target_key = source_key_map.get(src_key, src_key)

        for day, day_data in day_map.items():
            candidate = {
                "programmes": day_data["programmes"],
                "title_score": day_data["title_score"],
                "programme_count": day_data["programme_count"],
                "source_url": parsed["source_url"],
            }

            old_day = merged_days[target_key].get(day)
            if is_better_day(candidate, old_day):
                merged_days[target_key][day] = candidate


def finalize_merged_channels(merged_channels, merged_days):
    final_channels = {}

    for channel_key, meta in merged_channels.items():
        if channel_key not in merged_days or not merged_days[channel_key]:
            continue

        channel_name = meta["channel_name"] or next(iter(meta["display_names"]), channel_key)
        final_channels[channel_key] = {
            "channel_name": channel_name,
            "display_names": order_display_names(channel_name, meta["display_names"]),
        }

    return final_channels


def write_to_xml(channels, merged_days, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    root = ET.Element("tv", attrib={
        "date": datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    })

    sorted_channel_keys = sorted(channels.keys(), key=lambda k: channels[k]["channel_name"])

    for channel_key in sorted_channel_keys:
        channel_elem = ET.SubElement(root, "channel", attrib={"id": channel_key})

        for display_name, lang in channels[channel_key]["display_names"].items():
            attrs = {}
            if lang:
                attrs["lang"] = lang
            dn = ET.SubElement(channel_elem, "display-name", attrib=attrs)
            dn.text = display_name

    for channel_key in sorted_channel_keys:
        day_map = merged_days.get(channel_key, {})
        for day in sorted(day_map.keys()):
            programmes = sorted(day_map[day]["programmes"], key=lambda p: (p["start"], p["stop"]))
            for prog in programmes:
                prog_elem = ET.SubElement(root, "programme", attrib={
                    "channel": channel_key,
                    "start": format_xmltv_datetime(prog["start"]),
                    "stop": format_xmltv_datetime(prog["stop"]),
                })

                for title, lang in prog["titles"]:
                    attrs = {}
                    if lang:
                        attrs["lang"] = lang
                    title_elem = ET.SubElement(prog_elem, "title", attrib=attrs)
                    title_elem.text = title

    tree = ET.ElementTree(root)
    tree.write(filename, encoding="utf-8", xml_declaration=True)


def write_source_log(channels, merged_days, filename):
    with open(filename, "w", encoding="utf-8") as f:
        for channel_key in sorted(channels.keys(), key=lambda k: channels[k]["channel_name"]):
            channel_name = channels[channel_key]["channel_name"]
            for day in sorted(merged_days.get(channel_key, {}).keys()):
                source_url = merged_days[channel_key][day]["source_url"]
                f.write(
                    f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')} | 来源: {source_url}\n"
                )


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, "rb") as f_in:
        with gzip.open(output_filename, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    urls = []
    if not os.path.exists(CONFIG_PATH):
        return urls

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


async def main():
    urls = get_urls()
    if not urls:
        print("config.txt 中没有可用的 EPG 源。")
        return

    alias_resolver = AliasResolver(ALIAS_PATH)

    connector = aiohttp.TCPConnector(limit=16, ssl=False, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=90, sock_connect=15, sock_read=60)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/113.0.0.0 Safari/537.36"
        )
    }

    sem = asyncio.Semaphore(16)

    print("Fetching EPG data...")
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=True,
        headers=headers
    ) as session:
        tasks = [fetch_epg(session, url, sem) for url in urls]
        fetched_results = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    merged_channels = {}
    merged_days = defaultdict(dict)
    global_match_map = {}
    conflict_match_keys = set()

    print("Parsing and merging EPG...")
    for source_url, epg_content in tqdm(fetched_results, desc="Parsing & Merging", unit="source"):
        if not epg_content:
            continue

        parsed = parse_epg(epg_content, source_url, alias_resolver)
        if not parsed:
            continue

        merge_source_into_global(
            parsed,
            merged_channels,
            merged_days,
            global_match_map,
            conflict_match_keys,
            alias_resolver
        )

    final_channels = finalize_merged_channels(merged_channels, merged_days)

    if not final_channels:
        print("没有生成任何可用的 EPG 数据。")
        return

    print("Writing XML...")
    write_to_xml(final_channels, merged_days, OUTPUT_XML)

    print("Writing source log...")
    write_source_log(final_channels, merged_days, LOG_PATH)

    print("Compressing GZ...")
    compress_to_gz(OUTPUT_XML, OUTPUT_GZ)

    print("Done.")
    print(f"XML: {OUTPUT_XML}")
    print(f"GZ : {OUTPUT_GZ}")
    print(f"LOG: {LOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
