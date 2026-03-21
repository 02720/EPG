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
from io import BytesIO

TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# --- OpenCC singleton ---
_cc = OpenCC("t2s")


def transform2_zh_hans(string):
    if not string:
        return string
    return _cc.convert(string)


# --- URL pattern for filtering website homepages from programme titles ---
_URL_PATTERN = re.compile(
    r'^\s*https?://[^\s]+\s*$|^\s*www\.[^\s]+\s*$', re.IGNORECASE
)


def is_url_content(text):
    """Check if text is a URL (website homepage inserted as programme)."""
    if not text:
        return False
    return bool(_URL_PATTERN.match(text.strip()))


# --- Alias loading ---
def load_aliases(alias_file='alias.txt'):
    """
    Load channel aliases from alias.txt.
    Format: 主名,别名1,别名2,...
    Aliases prefixed with re: are treated as regex patterns.
    Lines starting with # are ignored.
    Returns:
        primary_map: dict mapping exact alias string -> primary name
        regex_aliases: list of (compiled_regex, primary_name)
    """
    primary_map = {}
    regex_aliases = []

    if not os.path.exists(alias_file):
        return primary_map, regex_aliases

    with open(alias_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = [p.strip() for p in line.split(',')]
            if len(parts) < 2:
                continue
            primary = parts[0]
            # Map primary name to itself
            primary_map[primary] = primary
            for alias in parts[1:]:
                if alias.startswith('re:'):
                    pattern = alias[3:]
                    try:
                        compiled = re.compile(pattern)
                        regex_aliases.append((compiled, primary))
                    except re.error as e:
                        print(f"Warning: Invalid regex pattern '{pattern}': {e}")
                else:
                    primary_map[alias] = primary

    return primary_map, regex_aliases


def resolve_alias(name, primary_map, regex_aliases):
    """Resolve a channel name to its primary name using aliases."""
    if name in primary_map:
        return primary_map[name]
    for pattern, primary in regex_aliases:
        if pattern.match(name):
            return primary
    return name


# --- Logging setup ---
def setup_logger():
    logger = logging.getLogger('epg_source')
    logger.setLevel(logging.INFO)
    # Clear existing handlers
    logger.handlers.clear()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_file = os.path.join(script_dir, 'epg_source.log')

    fh = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    fh.setLevel(logging.INFO)
    formatter = logging.Formatter('%(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


async def fetch_epg(url):
    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(
            connector=connector, trust_env=True, headers=headers, timeout=timeout
        ) as session:
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


def process_display_name(display_name):
    """Remove '高清' suffix but keep '超高清'."""
    if not display_name:
        return display_name
    # Only remove '高清' when NOT preceded by '超'
    display_name = re.sub(r'(?<!超)高清$', '', display_name)
    return display_name


def parse_datetime_safe(dt_str):
    """
    Safely parse datetime string in format YYYYMMDDHHMMSS with optional timezone offset.
    Handles empty strings, missing timezone, and invalid dates.
    Returns datetime with timezone or None on failure.
    """
    if not dt_str:
        return None

    dt_str = re.sub(r'\s+', '', dt_str.strip())
    if not dt_str:
        return None

    # Try with timezone offset (e.g., +0800)
    # Pattern: 14 digits optionally followed by +/- and 4 digits
    match = re.match(r'^(\d{14})([+-]\d{4})?$', dt_str)
    if not match:
        return None

    date_part = match.group(1)
    tz_part = match.group(2)

    try:
        if tz_part:
            full_str = date_part + ' ' + tz_part
            return datetime.strptime(full_str, "%Y%m%d%H%M%S %z")
        else:
            # No timezone info, assume UTC+8
            dt = datetime.strptime(date_part, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=TZ_UTC_PLUS_8)
    except ValueError:
        return None


def parse_epg(epg_content, source_url=""):
    """Parse EPG XML content. Returns channels dict and programmes dict."""
    try:
        # Remove BOM if present
        if epg_content.startswith('\ufeff'):
            epg_content = epg_content[1:]

        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content.encode('utf-8'), parser=parser)
    except ET.ParseError as e:
        print(f"Error parsing XML from {source_url}: {e}")
        return {}, defaultdict(list)

    channels = {}
    programmes = defaultdict(list)

    for channel in root.findall('channel'):
        channel_id = transform2_zh_hans(channel.get('id', ''))
        channel_display_names = []
        for name in channel.findall('display-name'):
            if name.text is None:
                continue
            t_name = transform2_zh_hans(name.text.strip())
            t_name = process_display_name(t_name)
            if t_name:
                channel_display_names.append([t_name, name.get('lang', 'zh')])
        if channel_id and not channel_id.isdigit():
            processed_id = process_display_name(transform2_zh_hans(channel_id))
            # Check if processed_id is already in display names
            existing_names = {dn[0] for dn in channel_display_names}
            if processed_id not in existing_names:
                channel_display_names.append([processed_id, 'zh'])
        channels[channel_id] = channel_display_names

    today = datetime.now(TZ_UTC_PLUS_8).date()
    valid_channels = set()

    for programme in root.findall('programme'):
        channel_id = transform2_zh_hans(programme.get('channel', ''))

        channel_start = parse_datetime_safe(programme.get('start', ''))
        channel_stop = parse_datetime_safe(programme.get('stop', ''))

        if channel_start is None or channel_stop is None:
            continue

        channel_start = channel_start.astimezone(TZ_UTC_PLUS_8)
        channel_stop = channel_stop.astimezone(TZ_UTC_PLUS_8)

        # Check if programme has valid title (not a URL)
        has_valid_title = False
        titles = []
        for title in programme.findall('title'):
            title_text = title.text.strip() if title.text else ""
            if is_url_content(title_text):
                continue
            if not title_text:
                title_text = "精彩节目"
            langattr = title.get('lang')
            if langattr == 'zh' or langattr is None:
                title_text = transform2_zh_hans(title_text)
            titles.append((title_text, langattr))
            has_valid_title = True

        if not has_valid_title:
            # If all titles were URLs, create a default title
            titles = [("精彩节目", None)]

        if channel_stop.date() >= today:
            valid_channels.add(channel_id)

        # Build programme element
        prog_elem = ET.Element(
            'programme',
            attrib={
                "start": channel_start.strftime("%Y%m%d%H%M%S %z"),
                "stop": channel_stop.strftime("%Y%m%d%H%M%S %z"),
            }
        )

        for title_text, langattr in titles:
            title_elem = ET.SubElement(prog_elem, 'title')
            title_elem.text = title_text
            if langattr is not None:
                title_elem.set('lang', langattr)

        for desc in programme.findall('desc'):
            if desc.text is None:
                continue
            desc_text = desc.text.strip()
            if not desc_text or is_url_content(desc_text):
                continue
            langattr = desc.get('lang')
            if langattr == 'zh' or langattr is None:
                desc_text = transform2_zh_hans(desc_text)
            desc_elem = ET.SubElement(prog_elem, 'desc')
            desc_elem.text = desc_text
            if langattr is not None:
                desc_elem.set('lang', langattr)

        programmes[channel_id].append(prog_elem)

    # Filter channels that have valid programmes
    channels = {k: v for k, v in channels.items() if k in valid_channels}
    programmes = {k: v for k, v in programmes.items() if k in valid_channels}

    return channels, programmes


def get_programme_date(prog_elem):
    """Get the date of a programme based on its start time."""
    start_str = prog_elem.get('start', '')
    dt = parse_datetime_safe(start_str)
    if dt:
        return dt.astimezone(TZ_UTC_PLUS_8).date()
    return None


def compute_daily_title_lengths(programmes_list):
    """
    Compute total title text length per day for a list of programme elements.
    Returns dict: date -> total_title_length
    """
    daily = defaultdict(int)
    for prog in programmes_list:
        d = get_programme_date(prog)
        if d is None:
            continue
        for title in prog.findall('title'):
            if title.text:
                daily[d] += len(title.text)
    return daily


def merge_programmes_by_day(existing_progs, new_progs):
    """
    Merge two programme lists by comparing daily title lengths.
    For each day, keep the source with longer total title text.
    Returns merged list and set of dates that were replaced by new_progs.
    """
    existing_by_day = defaultdict(list)
    new_by_day = defaultdict(list)

    for prog in existing_progs:
        d = get_programme_date(prog)
        if d is not None:
            existing_by_day[d].append(prog)

    for prog in new_progs:
        d = get_programme_date(prog)
        if d is not None:
            new_by_day[d].append(prog)

    all_dates = set(existing_by_day.keys()) | set(new_by_day.keys())
    result = []
    replaced_dates = set()  # dates where new source won

    for d in sorted(all_dates):
        e_progs = existing_by_day.get(d, [])
        n_progs = new_by_day.get(d, [])

        if not e_progs:
            result.extend(n_progs)
            replaced_dates.add(d)
        elif not n_progs:
            result.extend(e_progs)
        else:
            # Compare total title lengths
            e_title_len = sum(
                len(title.text) for prog in e_progs for title in prog.findall('title') if title.text
            )
            n_title_len = sum(
                len(title.text) for prog in n_progs for title in prog.findall('title') if title.text
            )
            if n_title_len > e_title_len:
                result.extend(n_progs)
                replaced_dates.add(d)
            else:
                result.extend(e_progs)

    return result, replaced_dates


def write_to_xml(channels_id, channels_names, programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')

    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    # Sort channels for consistent output
    sorted_ids = sorted(channels_id)

    for channel_id in sorted_ids:
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": channel_id})
        for display_name_node in channels_names[channel_id]:
            display_name = display_name_node[0]
            langattr = display_name_node[1]
            dn_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": langattr})
            dn_elem.text = display_name

    for channel_id in sorted_ids:
        if channel_id not in programmes:
            continue
        # Sort programmes by start time
        progs = sorted(programmes[channel_id], key=lambda p: p.get('start', ''))
        for prog in progs:
            prog.set('channel', channel_id)
            root.append(prog)

    # Write XML efficiently using ElementTree directly
    tree = ET.ElementTree(root)
    ET.indent(tree, space='\t')
    tree.write(filename, encoding='unicode', xml_declaration=True)


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    urls = []
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


def get_primary_display_name(display_names):
    """Get the first display name as the primary/representative name for a channel."""
    if display_names:
        return display_names[0][0]
    return "Unknown"


async def main():
    # Setup logger
    logger = setup_logger()

    # Load aliases
    primary_map, regex_aliases = load_aliases()
    print(f"Loaded {len(primary_map)} exact aliases and {len(regex_aliases)} regex aliases.")

    urls = get_urls()
    tasks = [fetch_epg(url) for url in urls]
    print("Fetching EPG data...")
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="Fetching URLs")

    all_channels_map = {}  # display_name -> primary channel_id
    all_channel_id = set()
    all_channel_names = defaultdict(list)
    all_programmes = defaultdict(list)

    # Track source URLs for each channel's daily programmes
    # source_tracking: channel_primary_id -> date -> source_url
    source_tracking = defaultdict(dict)

    print("Finished fetching.")

    for idx, epg_content in enumerate(epg_contents):
        source_url = urls[idx] if idx < len(urls) else f"source_{idx}"
        print(f"Processing EPG source {idx + 1}/{len(epg_contents)}: {source_url}")

        if epg_content is None:
            print(f"  Skipped (no content).")
            continue

        print("  Parsing EPG data...")
        channels, programmes = parse_epg(epg_content, source_url)
        print(f"  Parsed {len(channels)} channels.")

        with tqdm(total=len(channels), desc="  Merging EPG", unit="ch") as pbar:
            for channel_id, display_names in channels.items():
                if len(programmes.get(channel_id, [])) == 0:
                    pbar.update(1)
                    continue

                # --- Alias resolution ---
                # Resolve all display names through alias system to find primary name
                resolved_names = set()
                for dn_node in display_names:
                    dn = dn_node[0]
                    resolved = resolve_alias(dn, primary_map, regex_aliases)
                    resolved_names.add(resolved)

                # Also resolve the channel_id itself
                resolved_id = resolve_alias(
                    process_display_name(transform2_zh_hans(channel_id)),
                    primary_map, regex_aliases
                )
                resolved_names.add(resolved_id)

                # Check if any resolved name is already in our map
                is_in_map = False
                map_id = ""
                for rname in resolved_names:
                    if rname in all_channels_map:
                        is_in_map = True
                        map_id = all_channels_map[rname]
                        break

                # Also check original display names
                if not is_in_map:
                    for dn_node in display_names:
                        dn = dn_node[0]
                        if dn in all_channels_map:
                            is_in_map = True
                            map_id = all_channels_map[dn]
                            break

                if not is_in_map:
                    # Use the first resolved name or first display name as primary ID
                    map_id = resolved_id if resolved_id else (
                        display_names[0][0] if display_names else channel_id
                    )
                    all_channel_id.add(map_id)
                    all_channel_names[map_id] = list(display_names)
                    all_programmes[map_id] = programmes[channel_id]

                    # Register all names in the map
                    for dn_node in display_names:
                        all_channels_map[dn_node[0]] = map_id
                    for rname in resolved_names:
                        all_channels_map[rname] = map_id

                    # Track source for all dates in this channel's programmes
                    for prog in programmes[channel_id]:
                        d = get_programme_date(prog)
                        if d is not None:
                            source_tracking[map_id][d] = source_url

                else:
                    # Merge by day: compare daily title lengths
                    merged, replaced_dates = merge_programmes_by_day(
                        all_programmes[map_id], programmes[channel_id]
                    )
                    all_programmes[map_id] = merged

                    # Update source tracking for replaced dates
                    for d in replaced_dates:
                        source_tracking[map_id][d] = source_url

                    # Add new display names
                    existing_names = {dn[0] for dn in all_channel_names[map_id]}
                    for dn_node in display_names:
                        dn = dn_node[0]
                        if dn not in existing_names:
                            all_channel_names[map_id].append(dn_node)
                            existing_names.add(dn)
                        if dn not in all_channels_map:
                            all_channels_map[dn] = map_id
                    for rname in resolved_names:
                        if rname not in all_channels_map:
                            all_channels_map[rname] = map_id

                pbar.update(1)

    # --- Write source log ---
    print("Writing source log...")
    for channel_id in sorted(all_channel_id):
        display_name = get_primary_display_name(all_channel_names.get(channel_id, []))
        dates = sorted(source_tracking.get(channel_id, {}).keys())
        for d in dates:
            src = source_tracking[channel_id][d]
            logger.info(
                f"频道: {display_name} | 日期: {d.strftime('%Y-%m-%d')} | 来源: {src}"
            )

    print("Writing to XML...")
    write_to_xml(all_channel_id, all_channel_names, all_programmes, 'output/epg.xml')
    print("Compressing to .gz...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("Done!")


if __name__ == '__main__':
    asyncio.run(main())
