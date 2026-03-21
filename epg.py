import asyncio
import gzip
import io
import os
import re
import shutil
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse

import aiohttp
from opencc import OpenCC
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
CONFIG_FILE = os.path.join(BASE_DIR, "config.txt")
ALIAS_FILE = os.path.join(BASE_DIR, "alias.txt")
LOG_FILE = os.path.join(BASE_DIR, "epg_source.log")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_XML = os.path.join(OUTPUT_DIR, "epg.xml")
OUTPUT_GZ = os.path.join(OUTPUT_DIR, "epg.gz")

CC = OpenCC("t2s")

URL_RE = re.compile(r'(?i)\b(?:https?://|www\.)[^\s<>"\']+')
CONTROL_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
TIME_RE = re.compile(r'^\s*(\d{8}|\d{12}|\d{14})(?:\s*([+-]\d{2}:?\d{2}|Z))?\s*$')


@dataclass
class Programme:
    start: datetime
    stop: datetime
    titles: List[Tuple[str, str]]
    primary_title: str


@dataclass
class SourceChannel:
    preferred_name: str
    aliased: bool = False
    display_names: List[Tuple[str, str]] = field(default_factory=list)
    match_keys: Set[str] = field(default_factory=set)
    daily_programmes: Dict = field(default_factory=lambda: defaultdict(list))


@dataclass
class GlobalChannel:
    channel_id: str
    preferred_name: str
    display_names: List[Tuple[str, str]] = field(default_factory=list)
    match_keys: Set[str] = field(default_factory=set)
    daily_programmes: Dict = field(default_factory=dict)
    daily_scores: Dict = field(default_factory=dict)
    daily_sources: Dict = field(default_factory=dict)


def local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if isinstance(tag, str) else tag


def is_zh_lang(lang: str) -> bool:
    return not lang or lang.lower().startswith("zh")


def is_meaningful_name(name: str) -> bool:
    if not name:
        return False
    name = name.strip()
    if not name or name.isdigit():
        return False
    if len(name) == 1:
        return False
    return True


def is_punctuation_only(text: str) -> bool:
    if not text:
        return False
    for ch in text:
        if ch.isspace():
            continue
        cat = unicodedata.category(ch)
        if not (cat.startswith("P") or cat.startswith("S")):
            return False
    return True


@lru_cache(maxsize=200000)
def transform2_zh_hans(text: str) -> str:
    if not text:
        return ""
    return CC.convert(text)


def process_display_name(display_name: str) -> str:
    if not display_name:
        return ""
    display_name = display_name.strip()
    # 去掉“高清”，但保留“超高清”
    display_name = re.sub(r'(?<!超)高清$', '', display_name)
    return display_name.strip()


@lru_cache(maxsize=200000)
def normalize_channel_name(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = CONTROL_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""
    text = transform2_zh_hans(text)
    text = process_display_name(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def clean_program_text(text: str, lang: str = None) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = CONTROL_RE.sub("", text)
    text = URL_RE.sub("", text)  # 屏蔽节目单中的网址
    text = re.sub(r"\s+", " ", text).strip(" \t\r\n-—|丨/\\[]()（）【】<>《》")
    if not text or is_punctuation_only(text):
        return ""
    if is_zh_lang(lang):
        text = transform2_zh_hans(text)
    text = text.strip()
    if not text or is_punctuation_only(text):
        return ""
    return text


def exact_match_key(name: str) -> str:
    normalized = normalize_channel_name(name)
    return normalized.upper() if normalized else ""


def build_loose_key(name: str) -> str:
    normalized = normalize_channel_name(name)
    if not normalized or normalized.isdigit():
        return ""
    normalized = unicodedata.normalize("NFKC", normalized).upper()
    normalized = re.sub(r"[\s\-_·•・．.]+", "", normalized)
    return normalized


def build_match_keys(names: List[str]) -> Set[str]:
    keys = set()
    for name in names:
        if not is_meaningful_name(name):
            continue
        e_key = exact_match_key(name)
        if e_key:
            keys.add(f"E:{e_key}")
        l_key = build_loose_key(name)
        if l_key:
            keys.add(f"L:{l_key}")
    return keys


def unique_display_names(preferred_name: str, names: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    result = []
    seen = set()

    merged = []
    if preferred_name:
        merged.append((preferred_name, "zh"))
    merged.extend(names or [])

    for name, lang in merged:
        normalized = normalize_channel_name(name)
        lang = lang or "zh"
        if not normalized:
            continue
        key = (normalized, lang)
        if key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def merge_display_names(existing: List[Tuple[str, str]], preferred_name: str, new_names: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    return unique_display_names(preferred_name, list(existing or []) + list(new_names or []))


def refresh_match_keys(channel_obj) -> None:
    names = [channel_obj.preferred_name] + [name for name, _ in channel_obj.display_names]
    channel_obj.match_keys = build_match_keys(names)


def choose_primary_title(titles: List[Tuple[str, str]]) -> str:
    for text, lang in titles:
        if is_zh_lang(lang):
            return text
    return titles[0][0] if titles else ""


def programme_title_weight(programme: Programme) -> int:
    return sum(len(title) for title, _ in programme.titles)


def dedupe_and_sort_programmes(programmes: List[Programme]) -> List[Programme]:
    best = {}
    for prog in programmes:
        key = (prog.start, prog.stop, prog.primary_title)
        current = best.get(key)
        if current is None or programme_title_weight(prog) > programme_title_weight(current):
            best[key] = prog
    return sorted(best.values(), key=lambda x: (x.start, x.stop, x.primary_title))


def daily_title_score(programmes: List[Programme]) -> int:
    return sum(programme_title_weight(prog) for prog in programmes)


def parse_xmltv_time(value: str):
    if not value:
        return None

    value = value.strip()
    match = TIME_RE.match(value)
    if not match:
        return None

    digits, tz_str = match.groups()

    try:
        if len(digits) == 14:
            dt = datetime.strptime(digits, "%Y%m%d%H%M%S")
        elif len(digits) == 12:
            dt = datetime.strptime(digits, "%Y%m%d%H%M")
        elif len(digits) == 8:
            dt = datetime.strptime(digits, "%Y%m%d")
        else:
            return None
    except ValueError:
        return None

    if tz_str == "Z":
        dt = dt.replace(tzinfo=timezone.utc)
    elif tz_str:
        tz_str = tz_str.replace(":", "")
        sign = 1 if tz_str[0] == "+" else -1
        hours = int(tz_str[1:3])
        minutes = int(tz_str[3:5])
        offset = timedelta(hours=hours, minutes=minutes) * sign
        dt = dt.replace(tzinfo=timezone(offset))
    else:
        # 无时区时，默认按 +0800 处理
        dt = dt.replace(tzinfo=TZ_UTC_PLUS_8)

    return dt.astimezone(TZ_UTC_PLUS_8)


def format_xmltv_time(dt: datetime) -> str:
    return dt.astimezone(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")


def build_source_tag(url: str, index: int) -> str:
    parsed = urlparse(url)
    raw = f"{parsed.netloc}{parsed.path}".strip("/") or f"source{index}"
    return re.sub(r"[^0-9A-Za-z._-]+", "_", raw)


def get_urls() -> List[str]:
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"未找到配置文件: {CONFIG_FILE}")

    urls = []
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


def compress_to_gz(input_filename: str, output_filename: str) -> None:
    with open(input_filename, "rb") as f_in:
        with gzip.open(output_filename, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)


class AliasResolver:
    def __init__(self):
        self.exact_map: Dict[str, str] = {}
        self.regex_rules: List[Tuple[re.Pattern, str]] = []

    def add_rule(self, main_name: str, aliases: List[str]) -> None:
        main_name = normalize_channel_name(main_name)
        if not main_name:
            return

        self.exact_map[exact_match_key(main_name)] = main_name

        for alias in aliases:
            if not alias:
                continue
            alias = alias.strip()
            if not alias:
                continue

            if alias.startswith("re:"):
                pattern_text = alias[3:].strip()
                if not pattern_text:
                    continue
                try:
                    self.regex_rules.append((re.compile(pattern_text), main_name))
                except re.error as e:
                    print(f"alias.txt 正则无效: {pattern_text} | 错误: {e}")
            else:
                key = exact_match_key(alias)
                if key:
                    self.exact_map[key] = main_name

    def resolve(self, candidates: List[str]) -> str:
        processed = []
        for name in candidates:
            if not name:
                continue
            raw = unicodedata.normalize("NFKC", str(name)).strip()
            normalized = normalize_channel_name(raw)
            if normalized:
                processed.append((raw, normalized))

        for _, normalized in processed:
            key = exact_match_key(normalized)
            if key in self.exact_map:
                return self.exact_map[key]

        for raw, normalized in processed:
            for sample in (raw, normalized):
                for pattern, main_name in self.regex_rules:
                    if pattern.search(sample):
                        return main_name

        return ""


def load_alias_resolver() -> AliasResolver:
    resolver = AliasResolver()
    if not os.path.exists(ALIAS_FILE):
        return resolver

    with open(ALIAS_FILE, "r", encoding="utf-8") as file:
        for line in file:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue

            parts = [item.strip() for item in raw.split(",") if item.strip()]
            if not parts:
                continue

            main_name = parts[0]
            aliases = parts[1:]
            resolver.add_rule(main_name, aliases)

    return resolver


async def fetch_epg(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.read()

                # 某些源是 .gz 文件本体，aiohttp 不一定自动解压
                if data[:2] == b"\x1f\x8b":
                    try:
                        data = gzip.decompress(data)
                    except OSError:
                        pass

                return url, data

        except aiohttp.ClientError as e:
            print(f"{url} HTTP请求错误: {e}")
        except asyncio.TimeoutError:
            print(f"{url} 请求超时")
        except Exception as e:
            print(f"{url} 其他错误: {e}")

    return url, None


async def fetch_all(urls: List[str]):
    connector = aiohttp.TCPConnector(limit=32, limit_per_host=8, ssl=False, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=60, connect=15, sock_connect=15, sock_read=60)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        )
    }
    semaphore = asyncio.Semaphore(16)

    async with aiohttp.ClientSession(
        connector=connector,
        trust_env=True,
        timeout=timeout,
        headers=headers,
    ) as session:
        tasks = [fetch_epg(session, url, semaphore) for url in urls]
        return await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")


def parse_epg(epg_bytes: bytes, source_url: str, source_index: int, alias_resolver: AliasResolver) -> Dict[str, SourceChannel]:
    channels: Dict[str, SourceChannel] = {}
    channel_ref_map: Dict[str, str] = {}
    source_tag = build_source_tag(source_url, source_index)

    def ensure_channel(local_key: str, preferred_name: str, aliased: bool = False) -> SourceChannel:
        preferred_name_norm = normalize_channel_name(preferred_name) or preferred_name
        channel = channels.get(local_key)
        if channel is None:
            channel = SourceChannel(preferred_name=preferred_name_norm, aliased=aliased)
            channels[local_key] = channel
        else:
            if aliased:
                channel.aliased = True
                channel.preferred_name = preferred_name_norm
            elif (not is_meaningful_name(channel.preferred_name)) and is_meaningful_name(preferred_name_norm):
                channel.preferred_name = preferred_name_norm
        return channel

    def parse_stream(stream):
        for _, elem in ET.iterparse(stream, events=("end",)):
            tag = local_name(elem.tag)

            if tag == "channel":
                raw_id = (elem.get("id") or "").strip()
                norm_id = normalize_channel_name(raw_id)

                display_names = []
                candidates = []

                for child in list(elem):
                    if local_name(child.tag) != "display-name":
                        continue
                    raw_text = child.text or ""
                    norm_text = normalize_channel_name(raw_text)
                    if norm_text:
                        display_names.append((norm_text, child.get("lang") or "zh"))
                        candidates.append(raw_text)
                        candidates.append(norm_text)

                if norm_id and not norm_id.isdigit():
                    display_names.append((norm_id, "zh"))
                    candidates.append(raw_id)
                    candidates.append(norm_id)

                alias_main = alias_resolver.resolve(candidates)
                preferred_name = alias_main or next(
                    (name for name, _ in display_names if is_meaningful_name(name)),
                    norm_id or raw_id
                )

                preferred_name = normalize_channel_name(preferred_name) if preferred_name else ""
                if not preferred_name:
                    preferred_name = f"{source_tag}:channel_{len(channels) + 1}"

                if not is_meaningful_name(preferred_name) and not alias_main:
                    preferred_name = f"{source_tag}:{raw_id or len(channels) + 1}"

                local_key = alias_main or preferred_name
                channel = ensure_channel(local_key, alias_main or preferred_name, aliased=bool(alias_main))
                channel.display_names = merge_display_names(channel.display_names, channel.preferred_name, display_names)
                refresh_match_keys(channel)

                if raw_id:
                    channel_ref_map[raw_id] = local_key
                if norm_id:
                    channel_ref_map[norm_id] = local_key

                elem.clear()

            elif tag == "programme":
                raw_channel_ref = (elem.get("channel") or "").strip()
                norm_ref = normalize_channel_name(raw_channel_ref)

                local_key = channel_ref_map.get(raw_channel_ref) or channel_ref_map.get(norm_ref)

                if local_key is None:
                    alias_main = alias_resolver.resolve([raw_channel_ref, norm_ref])
                    preferred_name = alias_main or (
                        norm_ref if is_meaningful_name(norm_ref) else f"{source_tag}:{raw_channel_ref or 'unknown'}"
                    )
                    local_key = alias_main or preferred_name
                    channel = ensure_channel(local_key, alias_main or preferred_name, aliased=bool(alias_main))

                    name_candidates = []
                    if norm_ref:
                        name_candidates.append((norm_ref, "zh"))
                    elif raw_channel_ref:
                        name_candidates.append((raw_channel_ref, "zh"))

                    channel.display_names = merge_display_names(channel.display_names, channel.preferred_name, name_candidates)
                    refresh_match_keys(channel)

                    if raw_channel_ref:
                        channel_ref_map[raw_channel_ref] = local_key
                    if norm_ref:
                        channel_ref_map[norm_ref] = local_key

                start = parse_xmltv_time(elem.get("start"))
                stop = parse_xmltv_time(elem.get("stop"))

                if not start or not stop or stop <= start:
                    elem.clear()
                    continue

                titles = []
                title_seen = set()

                for child in list(elem):
                    if local_name(child.tag) != "title":
                        continue
                    lang = child.get("lang") or "zh"
                    title_text = clean_program_text(child.text, child.get("lang"))
                    if not title_text:
                        continue
                    key = (title_text, lang)
                    if key in title_seen:
                        continue
                    title_seen.add(key)
                    titles.append(key)

                # 标题为空（常见是只插了一个网址）则直接丢弃
                if not titles:
                    elem.clear()
                    continue

                primary_title = choose_primary_title(titles)
                prog = Programme(
                    start=start,
                    stop=stop,
                    titles=titles,
                    primary_title=primary_title,
                )

                channel = channels[local_key]
                day = start.date()
                channel.daily_programmes[day].append(prog)

                elem.clear()

    try:
        parse_stream(io.BytesIO(epg_bytes))
    except ET.ParseError:
        # 尝试再用清洗后的文本解析一次
        try:
            channels.clear()
            channel_ref_map.clear()
            text = epg_bytes.decode("utf-8", errors="ignore")
            text = CONTROL_RE.sub("", text).lstrip("\ufeff")
            parse_stream(io.StringIO(text))
        except ET.ParseError as e:
            preview = epg_bytes[:500].decode("utf-8", errors="ignore")
            print(f"Error parsing XML from {source_url}: {e}")
            print(f"Problematic content preview: {preview}")
            return {}

    final_channels = {}
    for key, channel in channels.items():
        if not channel.daily_programmes:
            continue

        channel.display_names = unique_display_names(channel.preferred_name, channel.display_names)
        refresh_match_keys(channel)

        new_daily = {}
        for day, programmes in channel.daily_programmes.items():
            cleaned = dedupe_and_sort_programmes(programmes)
            if cleaned:
                new_daily[day] = cleaned

        if new_daily:
            channel.daily_programmes = new_daily
            final_channels[key] = channel

    return final_channels


def resolve_final_channel_key(source_channel: SourceChannel, global_match_map: Dict[str, Set[str]]) -> str:
    if source_channel.aliased and is_meaningful_name(source_channel.preferred_name):
        return source_channel.preferred_name

    exact_matches = set()
    loose_matches = set()

    for token in source_channel.match_keys:
        matched = global_match_map.get(token, set())
        if token.startswith("E:"):
            exact_matches.update(matched)
        elif token.startswith("L:"):
            loose_matches.update(matched)

    if source_channel.preferred_name in exact_matches:
        return source_channel.preferred_name

    if len(exact_matches) == 1:
        return next(iter(exact_matches))

    if not exact_matches and len(loose_matches) == 1:
        return next(iter(loose_matches))

    # 出现歧义时不自动并入，避免串台
    return source_channel.preferred_name


def merge_into_global(
    global_channels: Dict[str, GlobalChannel],
    global_match_map: Dict[str, Set[str]],
    source_channels: Dict[str, SourceChannel],
    source_url: str,
) -> None:
    for _, source_channel in source_channels.items():
        final_key = resolve_final_channel_key(source_channel, global_match_map)
        if not final_key:
            continue

        global_channel = global_channels.get(final_key)
        if global_channel is None:
            global_channel = GlobalChannel(
                channel_id=final_key,
                preferred_name=final_key,
            )
            global_channels[final_key] = global_channel

        if source_channel.aliased and is_meaningful_name(final_key):
            global_channel.preferred_name = final_key
        elif not is_meaningful_name(global_channel.preferred_name) and is_meaningful_name(source_channel.preferred_name):
            global_channel.preferred_name = source_channel.preferred_name

        global_channel.display_names = merge_display_names(
            global_channel.display_names,
            global_channel.preferred_name,
            source_channel.display_names,
        )
        refresh_match_keys(global_channel)

        for token in global_channel.match_keys:
            global_match_map[token].add(final_key)

        for day, programmes in source_channel.daily_programmes.items():
            score = daily_title_score(programmes)
            existing_programmes = global_channel.daily_programmes.get(day)
            existing_score = global_channel.daily_scores.get(day, -1)

            # 以“当天所有 title 总长度”为主比较
            # 若相同，则退化为比较节目数
            if (
                existing_programmes is None
                or score > existing_score
                or (score == existing_score and len(programmes) > len(existing_programmes))
            ):
                global_channel.daily_programmes[day] = programmes
                global_channel.daily_scores[day] = score
                global_channel.daily_sources[day] = source_url


def write_source_log(global_channels: Dict[str, GlobalChannel], filename: str) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        sorted_channels = sorted(
            global_channels.values(),
            key=lambda c: (c.preferred_name or c.channel_id).upper()
        )
        for channel in sorted_channels:
            channel_name = channel.preferred_name or channel.channel_id
            for day in sorted(channel.daily_sources.keys()):
                f.write(
                    f"频道: [{channel_name}] | 日期: {day.strftime('%Y-%m-%d')} | 来源: {channel.daily_sources[day]}\n"
                )


def write_to_xml(global_channels: Dict[str, GlobalChannel], filename: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    root = ET.Element(
        "tv",
        attrib={"date": datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")}
    )

    sorted_channels = sorted(
        global_channels.values(),
        key=lambda c: (c.preferred_name or c.channel_id).upper()
    )

    for channel in sorted_channels:
        if not channel.daily_programmes:
            continue

        channel_elem = ET.SubElement(root, "channel", attrib={"id": channel.channel_id})
        display_names = unique_display_names(channel.preferred_name, channel.display_names)
        for display_name, lang in display_names:
            attrs = {"lang": lang} if lang else {}
            display_name_elem = ET.SubElement(channel_elem, "display-name", attrib=attrs)
            display_name_elem.text = display_name

    for channel in sorted_channels:
        if not channel.daily_programmes:
            continue

        all_programmes = []
        for day in sorted(channel.daily_programmes.keys()):
            all_programmes.extend(channel.daily_programmes[day])

        all_programmes.sort(key=lambda x: (x.start, x.stop, x.primary_title))

        for prog in all_programmes:
            prog_elem = ET.SubElement(
                root,
                "programme",
                attrib={
                    "channel": channel.channel_id,
                    "start": format_xmltv_time(prog.start),
                    "stop": format_xmltv_time(prog.stop),
                },
            )

            for title, lang in prog.titles:
                attrs = {"lang": lang} if lang else {}
                title_elem = ET.SubElement(prog_elem, "title", attrib=attrs)
                title_elem.text = title

    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="\t")
    except AttributeError:
        pass
    tree.write(filename, encoding="utf-8", xml_declaration=True)


async def main():
    urls = get_urls()
    alias_resolver = load_alias_resolver()

    print(f"Fetching {len(urls)} EPG sources...")
    fetch_results = await fetch_all(urls)

    global_channels: Dict[str, GlobalChannel] = {}
    global_match_map: Dict[str, Set[str]] = defaultdict(set)

    for idx, (url, epg_bytes) in enumerate(
        tqdm(fetch_results, desc="Parsing & merging", unit="source"),
        start=1
    ):
        if not epg_bytes:
            continue

        source_channels = parse_epg(epg_bytes, url, idx, alias_resolver)
        if not source_channels:
            continue

        merge_into_global(global_channels, global_match_map, source_channels, url)

    # 清理空频道
    global_channels = {
        key: channel
        for key, channel in global_channels.items()
        if channel.daily_programmes
    }

    for channel in global_channels.values():
        channel.display_names = unique_display_names(channel.preferred_name, channel.display_names)
        refresh_match_keys(channel)

    print("Writing source log...")
    write_source_log(global_channels, LOG_FILE)

    print("Writing XML...")
    write_to_xml(global_channels, OUTPUT_XML)

    print("Compressing GZ...")
    compress_to_gz(OUTPUT_XML, OUTPUT_GZ)

    print("Done.")
    print(f"XML: {OUTPUT_XML}")
    print(f"GZ : {OUTPUT_GZ}")
    print(f"LOG: {LOG_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
