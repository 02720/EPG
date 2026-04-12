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
import logging

# ── 全局常量 ──────────────────────────────────────────────
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))
CC = OpenCC("t2s")          # 全局 OpenCC，避免重复创建

# ── 日志：epg_source.log ──────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH    = os.path.join(SCRIPT_DIR, "epg_source.log")

logging.basicConfig(
    filename=LOG_PATH,
    filemode="w",
    encoding="utf-8",
    level=logging.INFO,
    format="%(message)s",
)
source_logger = logging.getLogger("epg_source")


# ── 工具函数 ──────────────────────────────────────────────
def transform2_zh_hans(string: str) -> str:
    return CC.convert(string)


def process_display_name(display_name: str) -> str:
    """去除末尾'高清'，但保留'超高清'"""
    if display_name.endswith("高清") and not display_name.endswith("超高清"):
        display_name = display_name[:-2]
    return display_name


# ── 外文检测 ──────────────────────────────────────────────
_RE_CJK = re.compile(
    r"[\u4e00-\u9fff\u3400-\u4dbf\U00020000-\U0002a6df"
    r"\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]"
)
_RE_FOREIGN_CHAR = re.compile(r"[A-Za-z\u0080-\u024f\u0400-\u04ff]")


def _is_foreign_text(text: str) -> bool:
    """判断字符串是否为'纯外文'（无任何 CJK 字符）"""
    return bool(text.strip()) and not _RE_CJK.search(text)


def _foreign_ratio(text: str) -> float:
    """计算文本中外文字符占比（以字符数为基准）"""
    if not text:
        return 0.0
    foreign = len(_RE_FOREIGN_CHAR.findall(text))
    return foreign / len(text)


def is_foreign_channel(channel_name: str, programmes: list) -> bool:
    """
    满足以下任一条件即视为国外频道：
    1. 频道名为纯外文（无 CJK）
    2. 所有节目 title 合并后，外文字符占比 > 60%
    """
    if _is_foreign_text(channel_name):
        return True
    titles = []
    for prog in programmes:
        for t in prog.findall("title"):
            if t.text:
                titles.append(t.text)
    combined = "".join(titles)
    if combined and _foreign_ratio(combined) > 0.6:
        return True
    return False


# ── 别名加载 ──────────────────────────────────────────────
def load_aliases(filepath="alias.txt"):
    """
    返回:
        alias_to_main : dict  别名(str) -> 主名(str)
        regex_rules   : list  [(compiled_re, main_name), ...]
        main_to_aliases: dict 主名(str) -> [别名, ...]  (不含正则)
    """
    alias_to_main  = {}
    regex_rules    = []
    main_to_aliases = defaultdict(list)

    if not os.path.exists(filepath):
        return alias_to_main, regex_rules, main_to_aliases

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) < 2:
                continue
            main = parts[0]
            for alias in parts[1:]:
                if alias.startswith("re:"):
                    pattern = alias[3:]
                    try:
                        regex_rules.append((re.compile(pattern), main))
                    except re.error as e:
                        print(f"[alias] 正则表达式错误 '{pattern}': {e}")
                else:
                    alias_to_main[alias] = main
                    main_to_aliases[main].append(alias)

    return alias_to_main, regex_rules, main_to_aliases


def resolve_main_name(name: str, alias_to_main: dict, regex_rules: list) -> str:
    """将任意名称解析为主名（找不到则返回原名）"""
    if name in alias_to_main:
        return alias_to_main[name]
    for pattern, main in regex_rules:
        if pattern.search(name):
            return main
    return name


# ── config.txt 读取（含白名单分区）────────────────────────
def get_urls():
    """
    返回:
        normal_urls    : list[str]  非白名单 URL
        whitelist_urls : list[str]  白名单 URL
    """
    normal_urls    = []
    whitelist_urls = []
    in_whitelist   = False

    if not os.path.exists("config.txt"):
        return normal_urls, whitelist_urls

    with open("config.txt", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line == "[WHITELIST]":
                in_whitelist = True
                continue
            if in_whitelist:
                whitelist_urls.append(line)
            else:
                normal_urls.append(line)

    return normal_urls, whitelist_urls


# ── 网络请求 ──────────────────────────────────────────────
# 屏蔽已知"网站主页插入节目单"的标志性字符串
_HOMEPAGE_TITLE_PATTERNS = [
    re.compile(r"由https?://\S+提供服务", re.IGNORECASE),
    re.compile(r"epg\.136605\.xyz",       re.IGNORECASE),
]


def _is_homepage_programme(programme_elem) -> bool:
    """若节目 title 含有主页推广信息则返回 True"""
    for t in programme_elem.findall("title"):
        text = (t.text or "").strip()
        for pat in _HOMEPAGE_TITLE_PATTERNS:
            if pat.search(text):
                return True
    return False


async def fetch_epg(url: str):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/113.0.0.0 Safari/537.36"
        )
    }
    try:
        async with aiohttp.ClientSession(
            connector=connector, trust_env=True, headers=headers
        ) as session:
            async with session.get(url) as response:
                if url.endswith(".gz"):
                    compressed_data = await response.read()
                    return gzip.decompress(compressed_data).decode("utf-8", errors="ignore")
                else:
                    return await response.text(encoding="utf-8")
    except aiohttp.ClientError as e:
        print(f"{url} HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"{url} 请求超时")
    except Exception as e:
        print(f"{url} 其他错误: {e}")
    return None


# ── 时间解析（容错）──────────────────────────────────────
def _parse_time(raw: str):
    """
    解析 XMLTV 时间字段，兼容：
      - 带时区：'20231001120000 +0800'  → 格式 '%Y%m%d%H%M%S %z'
      - 纯数字：'20231001120000'        → 格式 '%Y%m%d%H%M%S'（视为 UTC+8）
      - 空串 / 格式异常                 → 返回 None
    """
    s = re.sub(r"\s+", "", raw).strip()
    if not s:
        return None
    # 尝试带时区（去空格后时区紧跟，如 '20231001120000+0800'）
    # minidom/ET 的原始字段常含空格，已被去除，需重新插入空格后再解析
    # 先尝试含时区偏移（长度 > 14）
    if len(s) > 14:
        ts_part  = s[:14]
        tz_part  = s[14:]
        combined = ts_part + " " + tz_part
        try:
            return datetime.strptime(combined, "%Y%m%d%H%M%S %z")
        except ValueError:
            pass
    # 再尝试纯数字（14位）
    if len(s) >= 14:
        try:
            dt = datetime.strptime(s[:14], "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
        except ValueError:
            pass
    return None


# ── EPG 解析 ──────────────────────────────────────────────
def parse_epg(epg_content: str, source_url: str,
              alias_to_main: dict, regex_rules: list):
    """
    解析单个 EPG 源。
    返回:
        channels   : dict  channel_id(主名) -> [[display_name, lang], ...]
        programmes : dict  channel_id(主名) -> [programme_elem, ...]
    """
    try:
        parser = ET.XMLParser(encoding="UTF-8")
        root   = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"[{source_url}] XML 解析错误: {e}")
        return {}, defaultdict(list)

    channels   = {}
    programmes = defaultdict(list)

    # ── 解析频道 ──
    for channel in root.findall("channel"):
        raw_id = channel.get("id") or ""
        channel_id = transform2_zh_hans(raw_id)

        display_names = []
        for name in channel.findall("display-name"):
            if not name.text:
                continue
            t_name = transform2_zh_hans(name.text)
            t_name = process_display_name(t_name)
            display_names.append([t_name, name.get("lang", "zh")])

        if not channel_id.isdigit() and not any(n[0] == channel_id for n in display_names):
            display_names.append([channel_id, "zh"])

        # 别名 → 主名
        main_id = resolve_main_name(channel_id, alias_to_main, regex_rules)
        for dn in display_names:
            resolved = resolve_main_name(dn[0], alias_to_main, regex_rules)
            if resolved != dn[0]:
                main_id = resolved
                break

        channels[channel_id] = (main_id, display_names)

    # ── 解析节目 ──
    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channel_ids = set()

    for programme in root.findall("programme"):
        raw_channel = programme.get("channel") or ""
        channel_id  = transform2_zh_hans(raw_channel)

        raw_start = programme.get("start", "")
        raw_stop  = programme.get("stop",  "")
        channel_start = _parse_time(raw_start)
        channel_stop  = _parse_time(raw_stop)

        if channel_start is None or channel_stop is None:
            continue  # 跳过无法解析的时间

        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop  = channel_stop.astimezone(TZ_UTC_PLUS_8)

        # 屏蔽主页推广节目
        if _is_homepage_programme(programme):
            continue

        if channel_stop.date() >= today:
            valid_channel_ids.add(channel_id)

        # 构建新 programme 元素
        prog_elem = ET.Element(
            "programme",
            attrib={
                "start":   channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop":    channel_stop.strftime("%Y%m%d%H%M%S %z"),
                "channel": channel_id,
            },
        )
        for title in programme.findall("title"):
            raw_text    = (title.text or "").strip() or "精彩节目"
            langattr    = title.get("lang")
            title_text  = transform2_zh_hans(raw_text) if (langattr in ("zh", None)) else raw_text
            t_elem      = ET.SubElement(prog_elem, "title")
            t_elem.text = title_text
            if langattr is not None:
                t_elem.set("lang", langattr)

        for desc in programme.findall("desc"):
            if not desc.text:
                continue
            langattr   = desc.get("lang")
            desc_text  = desc.text.strip()
            desc_text  = transform2_zh_hans(desc_text) if (langattr in ("zh", None)) else desc_text
            d_elem     = ET.SubElement(prog_elem, "desc")
            d_elem.text = desc_text
            if langattr is not None:
                d_elem.set("lang", langattr)

        # 别名 → 主名 映射
        info = channels.get(channel_id)
        main_id = info[0] if info else resolve_main_name(channel_id, alias_to_main, regex_rules)

        programmes[main_id].append((channel_stop.date(), prog_elem))

    # 仅保留当日有效频道
    resolved_channels = {}
    for cid, (main_id, display_names) in channels.items():
        if cid in valid_channel_ids:
            resolved_channels[main_id] = display_names

    resolved_programmes = {}
    for main_id, progs in programmes.items():
        filtered = [(d, e) for d, e in progs if any(
            cid in valid_channel_ids for cid, (mid, _) in channels.items() if mid == main_id
        )]
        # 简化：只要 main_id 存在于 resolved_channels 则保留
        if main_id in resolved_channels:
            resolved_programmes[main_id] = [e for _, e in progs]

    return resolved_channels, resolved_programmes


# ── 节目信息量计算（以 title 总长度衡量，按天分组）────────
def _titles_by_day(programmes: list) -> dict:
    """
    返回 {date: title_total_length}
    programmes 元素为 ET.Element（programme）
    """
    day_titles = defaultdict(int)
    for prog in programmes:
        raw_start = prog.get("start", "")
        dt = _parse_time(raw_start)
        if dt is None:
            continue
        day = dt.astimezone(TZ_UTC_PLUS_8).date()
        for t in prog.findall("title"):
            day_titles[day] += len(t.text or "")
    return day_titles


def _info_score(programmes: list) -> dict:
    """返回 {date: title_length}，用于比较同一天哪个源信息更多"""
    return _titles_by_day(programmes)


# ── 外文频道过滤 ──────────────────────────────────────────
def _channel_display_name(display_names: list) -> str:
    """取第一个 display-name 作为频道名用于外文判断"""
    return display_names[0][0] if display_names else ""


# ── 主逻辑 ────────────────────────────────────────────────
async def main():
    alias_to_main, regex_rules, main_to_aliases = load_aliases("alias.txt")
    normal_urls, whitelist_urls = get_urls()
    all_urls = normal_urls + whitelist_urls

    print("Fetching EPG data...")
    tasks       = [fetch_epg(url) for url in all_urls]
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")
    print("Finished fetching.")

    normal_contents    = epg_contents[:len(normal_urls)]
    whitelist_contents = epg_contents[len(normal_urls):]

    # ── 数据结构 ──────────────────────────────────────────
    # 存储结构: channel_id(主名) -> {"names": [...], "progs": [...], "url": str}
    normal_data    = {}   # 非白名单合并结果
    whitelist_data = {}   # 白名单合并结果

    # ── 处理非白名单 ──────────────────────────────────────
    for idx, epg_content in enumerate(normal_contents):
        url = normal_urls[idx]
        print(f"Processing normal EPG [{idx+1}/{len(normal_contents)}]: {url}")
        if epg_content is None:
            continue
        channels, programmes = parse_epg(epg_content, url, alias_to_main, regex_rules)

        for main_id, display_names in channels.items():
            if main_id not in programmes or not programmes[main_id]:
                continue
            progs = programmes[main_id]

            # 外文过滤
            ch_name = _channel_display_name(display_names)
            if is_foreign_channel(ch_name, progs):
                continue

            if main_id not in normal_data:
                normal_data[main_id] = {
                    "names": display_names,
                    "progs": progs,
                    "url":   url,
                    # 按天记录最优来源 {date: (url, score)}
                    "day_source": {},
                }
                score = _info_score(progs)
                for day, s in score.items():
                    normal_data[main_id]["day_source"][day] = (url, s)
            else:
                # 按天比较 title 总长度，保留较多的
                existing = normal_data[main_id]
                new_score = _info_score(progs)
                all_days  = set(new_score.keys()) | set(existing["day_source"].keys())

                # 逐天比较，决定每天用哪个源
                # 简单策略：若新源在某天 title 更多，则整体替换该频道数据
                # 更精细：按天拼接（但 XML 元素已经是整体列表，拆分复杂）
                # 此处实现：以"多数天胜出"决定是否整体替换
                new_wins = 0
                old_wins = 0
                for day in all_days:
                    ns = new_score.get(day, 0)
                    os_ = existing["day_source"].get(day, (None, 0))[1]
                    if ns > os_:
                        new_wins += 1
                    elif os_ > ns:
                        old_wins += 1

                if new_wins > old_wins:
                    normal_data[main_id]["progs"] = progs
                    normal_data[main_id]["url"]   = url
                    for day, s in new_score.items():
                        normal_data[main_id]["day_source"][day] = (url, s)
                else:
                    # 更新 day_source 中新源胜出的天（即便整体不替换也记录来源）
                    for day in all_days:
                        ns  = new_score.get(day, 0)
                        os_ = existing["day_source"].get(day, (None, 0))[1]
                        if ns > os_:
                            normal_data[main_id]["day_source"][day] = (url, ns)

                # 补充 display_names
                existing_names = {n[0] for n in existing["names"]}
                for dn in display_names:
                    if dn[0] not in existing_names:
                        existing["names"].append(dn)

    # ── 处理白名单 ────────────────────────────────────────
    for idx, epg_content in enumerate(whitelist_contents):
        url = whitelist_urls[idx]
        print(f"Processing whitelist EPG [{idx+1}/{len(whitelist_contents)}]: {url}")
        if epg_content is None:
            continue
        channels, programmes = parse_epg(epg_content, url, alias_to_main, regex_rules)

        for main_id, display_names in channels.items():
            if main_id not in programmes or not programmes[main_id]:
                continue
            progs = programmes[main_id]

            # 外文过滤
            ch_name = _channel_display_name(display_names)
            if is_foreign_channel(ch_name, progs):
                continue

            if main_id not in whitelist_data:
                whitelist_data[main_id] = {
                    "names":      display_names,
                    "progs":      progs,
                    "url":        url,
                    "day_source": {},
                }
                score = _info_score(progs)
                for day, s in score.items():
                    whitelist_data[main_id]["day_source"][day] = (url, s)
            else:
                # 白名单内部也按天比较，保留信息多的
                existing  = whitelist_data[main_id]
                new_score = _info_score(progs)
                all_days  = set(new_score.keys()) | set(existing["day_source"].keys())
                new_wins  = sum(
                    1 for d in all_days
                    if new_score.get(d, 0) > existing["day_source"].get(d, (None, 0))[1]
                )
                old_wins  = sum(
                    1 for d in all_days
                    if existing["day_source"].get(d, (None, 0))[1] > new_score.get(d, 0)
                )
                if new_wins > old_wins:
                    existing["progs"] = progs
                    existing["url"]   = url
                    for day, s in new_score.items():
                        existing["day_source"][day] = (url, s)
                existing_names = {n[0] for n in existing["names"]}
                for dn in display_names:
                    if dn[0] not in existing_names:
                        existing["names"].append(dn)

    # ── 合并：白名单优先 ──────────────────────────────────
    final_data = {}
    for main_id, data in normal_data.items():
        final_data[main_id] = data
    for main_id, data in whitelist_data.items():
        # 白名单直接覆盖同频道非白名单数据
        final_data[main_id] = data

    # ── 写日志 ────────────────────────────────────────────
    for main_id, data in final_data.items():
        ch_name = _channel_display_name(data["names"])
        for day, (src_url, _) in sorted(data["day_source"].items()):
            source_logger.info(
                f"频道: [{ch_name}] | 日期: {day.strftime('%Y-%m-%d')} | 来源: {src_url}"
            )

    # ── 重新映射别名到输出 ────────────────────────────────
    # main_to_aliases: 主名 -> [别名, ...]（不含正则）
    for main_id, data in final_data.items():
        if main_id in main_to_aliases:
            existing_names = {n[0] for n in data["names"]}
            for alias in main_to_aliases[main_id]:
                if alias not in existing_names:
                    data["names"].append([alias, "zh"])

    # ── 输出 XML ──────────────────────────────────────────
    print("Writing to XML...")
    write_to_xml(
        list(final_data.keys()),
        {mid: d["names"] for mid, d in final_data.items()},
        {mid: d["progs"] for mid, d in final_data.items()},
        "output/epg.xml",
    )
    compress_to_gz("output/epg.xml", "output/epg.gz")
    print("Done.")


# ── XML 输出 ──────────────────────────────────────────────
def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists("output"):
        os.makedirs("output")
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element("tv", attrib={"date": current_time})

    for channel_id in channels_id:
        ch_elem = ET.SubElement(root, "channel", attrib={"id": channel_id})
        for dn_node in channels_names[channel_id]:
            dn_elem      = ET.SubElement(ch_elem, "display-name", attrib={"lang": dn_node[1]})
            dn_elem.text = dn_node[0]
        for prog in programmes[channel_id]:
            prog.set("channel", channel_id)
            root.append(prog)

    rough_string = ET.tostring(root, "utf-8")
    reparsed     = minidom.parseString(rough_string)
    with open(filename, "w", encoding="utf-8") as f:
        f.write(reparsed.toprettyxml(indent="\t", newl="\n"))


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, "rb") as f_in:
        with gzip.open(output_filename, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


if __name__ == "__main__":
    asyncio.run(main())
