import xml.etree.ElementTree as ET
from collections import defaultdict
import aiohttp
import asyncio
from tqdm.asyncio import tqdm_asyncio
from datetime import datetime, timezone, timedelta
import gzip
import shutil
import re
from opencc import OpenCC
import os
from tqdm import tqdm
import logging

# ── 常量 ─────────────────────────────────────────────────────────
TZ_UTC8 = timezone(timedelta(hours=8))

# ── 来源日志 (epg_source.log) ────────────────────────────────────
_src_log = logging.getLogger("epg_source")
_src_log.setLevel(logging.INFO)
_src_log.propagate = False
_src_fh = logging.FileHandler("epg_source.log", mode="w", encoding="utf-8")
_src_fh.setFormatter(logging.Formatter("%(message)s"))
_src_log.addHandler(_src_fh)

# ── OpenCC（带缓存）─────────────────────────────────────────────
_cc = OpenCC("t2s")
_t2s_cache = {}


def t2s(text):
    """繁→简，结果缓存"""
    if not text:
        return text
    out = _t2s_cache.get(text)
    if out is None:
        out = _cc.convert(text)
        _t2s_cache[text] = out
    return out


# ── URL 检测 ─────────────────────────────────────────────────────
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)


def _has_url(text):
    return bool(text and _URL_RE.search(text))


# ── 时间解析（兼容多种格式）─────────────────────────────────────
_TIME_FMTS = (
    "%Y%m%d%H%M%S%z",
    "%Y%m%d%H%M%S",
    "%Y%m%d%H%M%z",
    "%Y%m%d%H%M",
)


def _parse_time(raw):
    """解析时间字符串，失败返回 None；无时区时默认 UTC+8"""
    if not raw:
        return None
    raw = raw.replace(" ", "").strip()
    if not raw:
        return None
    for fmt in _TIME_FMTS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=TZ_UTC8)
        except ValueError:
            continue
    return None


# ── 辅助函数 ─────────────────────────────────────────────────────
def _strip_hd(name):
    if name and name.endswith("高清"):
        name = name[:-2]
    return name


def _day_title_len(progs):
    """一天内所有节目 title 文本总长度"""
    return sum(len(t) for p in progs for t, _ in p["titles"])


def _group_by_date(progs):
    d = defaultdict(list)
    for p in progs:
        d[p["start"].date()].append(p)
    return d


def _indent_xml(elem, level=0):
    """为 Python < 3.9 提供的 XML 缩进函数"""
    indent = "\n" + "\t" * level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = indent + "\t"
        if not elem.tail or not elem.tail.strip():
            elem.tail = indent
        for child in elem:
            _indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = indent
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = indent


# ── 网络请求 ─────────────────────────────────────────────────────
async def _fetch(session, url):
    try:
        async with session.get(url) as r:
            if url.endswith(".gz"):
                return gzip.decompress(await r.read()).decode("utf-8", errors="ignore")
            return await r.text(encoding="utf-8")
    except aiohttp.ClientError as e:
        print(f"  {url}  HTTP请求错误: {e}")
    except asyncio.TimeoutError:
        print(f"  {url}  请求超时")
    except Exception as e:
        print(f"  {url}  错误: {e}")
    return None


# ── XML 解析 ─────────────────────────────────────────────────────
def _parse(xml_text, src=""):
    """
    解析 EPG XML，返回:
        channels   {channel_id: [[name, lang], ...]}
        programmes {channel_id: [{start, stop, titles, descs}, ...]}
    使用 dict 存储节目信息（而非 ET.Element），避免修改源树、减少内存占用。
    """
    if not xml_text:
        return {}, {}
    # 去 BOM
    if xml_text[0] == "\ufeff":
        xml_text = xml_text[1:]
    # 去非法 XML 控制字符
    xml_text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", xml_text)

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"  XML解析失败 ({src}): {e}")
        return {}, {}

    # ── 频道 ──
    channels = {}
    for ch in root.findall("channel"):
        cid = t2s(ch.get("id", "").strip())
        names = []
        for dn in ch.findall("display-name"):
            txt = dn.text
            if not txt:
                continue
            txt = _strip_hd(t2s(txt.strip()))
            if not txt or _has_url(txt):
                continue
            names.append([txt, dn.get("lang", "zh")])
        if not names:
            continue
        # 非纯数字 channel_id 也作为别名
        if not cid.isdigit():
            pcid = _strip_hd(cid)
            if pcid and not any(n[0] == pcid for n in names):
                names.append([pcid, "zh"])
        channels[cid] = names

    # ── 节目 ──
    today = datetime.now(TZ_UTC8).date()
    valid = set()
    programmes = defaultdict(list)

    for pe in root.findall("programme"):
        cid = t2s(pe.get("channel", "").strip())
        # 防串台：仅保留在 channels 中已定义的频道节目
        if cid not in channels:
            continue
        start = _parse_time(pe.get("start"))
        stop = _parse_time(pe.get("stop"))
        if not start or not stop:
            continue
        start = start.astimezone(TZ_UTC8)
        stop = stop.astimezone(TZ_UTC8)

        # titles
        titles = []
        bad = False
        for te in pe.findall("title"):
            txt = (te.text or "").strip() or "精彩节目"
            if _has_url(txt):
                bad = True
                break
            lang = te.get("lang")
            if lang in ("zh", None):
                txt = t2s(txt)
            titles.append((txt, lang))
        if bad or not titles:
            continue

        # descs
        descs = []
        for de in pe.findall("desc"):
            txt = (de.text or "").strip()
            if not txt or _has_url(txt):
                continue
            lang = de.get("lang")
            if lang in ("zh", None):
                txt = t2s(txt)
            descs.append((txt, lang))

        programmes[cid].append(
            {"start": start, "stop": stop, "titles": titles, "descs": descs}
        )
        if stop.date() == today:
            valid.add(cid)

    # 仅保留今天有节目结束的频道（过滤过期源）
    channels = {k: v for k, v in channels.items() if k in valid}
    programmes = {k: v for k, v in programmes.items() if k in valid}
    return channels, programmes


# ── XML 写入 ─────────────────────────────────────────────────────
def _write_xml(ids, names, progs, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    root = ET.Element(
        "tv", date=datetime.now(TZ_UTC8).strftime("%Y%m%d%H%M%S %z")
    )
    ordered = sorted(ids)
    for cid in ordered:
        ce = ET.SubElement(root, "channel", id=cid)
        for n, l in names[cid]:
            de = ET.SubElement(ce, "display-name", lang=l)
            de.text = n
    for cid in ordered:
        for p in sorted(progs.get(cid, []), key=lambda x: x["start"]):
            pe = ET.SubElement(
                root,
                "programme",
                start=p["start"].strftime("%Y%m%d%H%M%S %z"),
                stop=p["stop"].strftime("%Y%m%d%H%M%S %z"),
                channel=cid,
            )
            for txt, lang in p["titles"]:
                te = ET.SubElement(pe, "title")
                te.text = txt
                if lang:
                    te.set("lang", lang)
            for txt, lang in p["descs"]:
                de = ET.SubElement(pe, "desc")
                de.text = txt
                if lang:
                    de.set("lang", lang)
    tree = ET.ElementTree(root)
    # Python 3.9+ 用 ET.indent，更早版本用自定义函数
    if hasattr(ET, "indent"):
        ET.indent(tree, space="\t")
    else:
        _indent_xml(root)
    tree.write(path, encoding="unicode", xml_declaration=True)


def _gzip_file(src, dst):
    with open(src, "rb") as fi, gzip.open(dst, "wb") as fo:
        shutil.copyfileobj(fi, fo)


def _urls():
    with open("config.txt", encoding="utf-8") as f:
        return [
            l.strip() for l in f if l.strip() and not l.lstrip().startswith("#")
        ]


# ── 主流程 ───────────────────────────────────────────────────────
async def main():
    urls = _urls()

    # ── 并发抓取（共享单一 Session，复用 TCP 连接）──
    print("Fetching EPG data …")
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=16, ssl=False),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/113.0.0.0 Safari/537.36"
            )
        },
        timeout=aiohttp.ClientTimeout(total=60),
        trust_env=True,
    ) as session:
        contents = await tqdm_asyncio.gather(
            *[_fetch(session, u) for u in urls], desc="Fetching"
        )
    print("Fetch complete.\n")

    # ── 合并状态 ──
    name2id = {}  # display_name → canonical_id
    cids = set()  # 已分配的 canonical id 集合
    cnames = {}  # canonical_id → [[name, lang], …]
    # canonical_id → {date → (progs_list, source_url)}
    by_id_dt = defaultdict(dict)

    for idx, raw in enumerate(contents):
        src = urls[idx]
        print(f"[{idx + 1}/{len(contents)}] {src}")
        if raw is None:
            continue
        channels, programmes = _parse(raw, src)
        if not channels:
            print("  (no valid channels)\n")
            continue
        print(f"  {len(channels)} valid channels")

        for ch_id, names in tqdm(
            channels.items(), desc="  Merging", unit="ch", leave=False
        ):
            progs = programmes.get(ch_id)
            if not progs:
                continue

            # 通过 display-name 查找已有的 canonical id
            canonical = None
            for n, _ in names:
                if n in name2id:
                    canonical = name2id[n]
                    break

            if canonical is None:
                # ── 新频道 ──
                canonical = ch_id
                # 防串台：若 canonical id 已被不同频道占用，加后缀区分
                if canonical in cids:
                    base, i = canonical, 2
                    while canonical in cids:
                        canonical = f"{base}_{i}"
                        i += 1
                cids.add(canonical)
                cnames[canonical] = [list(n) for n in names]
                for n, _ in names:
                    name2id[n] = canonical
            else:
                # ── 已有频道：补充别名 ──
                seen = {n[0] for n in cnames[canonical]}
                for n, l in names:
                    if n not in seen:
                        cnames[canonical].append([n, l])
                        seen.add(n)
                    if n not in name2id:
                        name2id[n] = canonical

            # 按天比较：保留 title 总长度更大的一方
            for dt, batch in _group_by_date(progs).items():
                new_len = _day_title_len(batch)
                cur = by_id_dt[canonical].get(dt)
                if cur is None or new_len > _day_title_len(cur[0]):
                    by_id_dt[canonical][dt] = (batch, src)
        print()

    # ── 扁平化 & 写日志 ──
    _src_log.info(
        "EPG Source Log  %s",
        datetime.now(TZ_UTC8).strftime("%Y-%m-%d %H:%M:%S"),
    )
    _src_log.info("=" * 100)
    final = defaultdict(list)
    for cid in sorted(cids):
        label = cnames[cid][0][0]
        for dt in sorted(by_id_dt[cid]):
            ps, src = by_id_dt[cid][dt]
            final[cid].extend(ps)
            _src_log.info(
                "频道: %-20s | 日期: %s | 节目数: %4d | title总长: %6d | 来源: %s",
                label,
                dt,
                len(ps),
                _day_title_len(ps),
                src,
            )
    _src_log.info("=" * 100)
    _src_log.info("共 %d 个频道", len(cids))

    # ── 输出 ──
    print("Writing XML …")
    _write_xml(cids, cnames, final, "output/epg.xml")
    _gzip_file("output/epg.xml", "output/epg.gz")
    print("Done!")


if __name__ == "__main__":
    asyncio.run(main())
