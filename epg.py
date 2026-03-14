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

# --- 全局设置 ---
TZ_UTC_PLUS_8 = timezone(timedelta(hours=8))

# 1. 提速：将 OpenCC 实例化改为全局单例，避免在循环中重复加载词典，大幅提升速度
cc = OpenCC("t2s")

# 5. 日志功能：配置日志输出格式
if os.path.exists('epg_source.log'):
    os.remove('epg_source.log')
logging.basicConfig(filename='epg_source.log', level=logging.INFO, format='%(message)s', encoding='utf-8')


# --- 辅助函数 ---
def transform2_zh_hans(string):
    if not string:
        return ""
    return cc.convert(string)

def clean_text_and_urls(text):
    """清理文本并过滤掉插入的网址 (Req 2)"""
    if not text:
        return ""
    text = transform2_zh_hans(text)
    # 正则匹配并删除 http:// 或 https:// 开头的网址
    text = re.sub(r'(?i)https?://[^\s]+', '', text)
    return text.strip()

def normalize_channel_id(name):
    """规范化频道ID，去除空格、破折号及清晰度标识，解决串台问题 (Req 6)"""
    name = clean_text_and_urls(name).upper()
    name = re.sub(r'高清|FHD|HD|标清|频道|-|\s', '', name)
    return name

def parse_epg_datetime(dt_str):
    """安全解析时间，修复 ValueError (Req 3)"""
    if not dt_str:
        return None
    # 去除内部的所有空格 (例如 '20260301000600 +0800')
    dt_str = dt_str.replace(" ", "")
    if not dt_str:
        return None
    try:
        # 如果长度为14，说明没有时区信息，默认补全北京时间 +0800
        if len(dt_str) == 14:
            dt_str += "+0800"
        dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S%z")
        return dt.astimezone(TZ_UTC_PLUS_8)
    except ValueError:
        return None


# --- 核心网络与解析逻辑 ---
async def fetch_epg(url):
    connector = aiohttp.TCPConnector(limit=16, ssl=False)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            async with session.get(url) as response:
                if url.endswith('.gz'):
                    compressed_data = await response.read()
                    content = gzip.decompress(compressed_data).decode('utf-8', errors='ignore')
                else:
                    content = await response.text(encoding='utf-8')
                return url, content  # 返回内容的同时附带URL，用于记录来源
    except Exception as e:
        print(f"\n{url} 请求或解析失败: {e}")
    return url, None


def parse_epg_content(epg_content, source_url):
    """解析单个源的内容，并计算每天节目的 title 总长度"""
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except Exception as e:
        return {}, {}, set()

    file_id_to_norm = {}
    channel_names = defaultdict(list)
    
    # 解析频道基本信息
    for channel in root.findall('channel'):
        file_id = channel.get('id')
        if not file_id: continue
        
        disp_names = channel.findall('display-name')
        best_name = disp_names[0].text if disp_names else file_id
        
        norm_id = normalize_channel_id(best_name) or file_id
        file_id_to_norm[file_id] = norm_id
        
        for name_elem in disp_names:
            t_name = clean_text_and_urls(name_elem.text)
            if t_name.endswith('高清'):
                t_name = t_name[:-2]
            lang = name_elem.get('lang', 'zh')
            channel_names[norm_id].append([t_name, lang])
            
        if not channel_names[norm_id]:
            channel_names[norm_id].append([clean_text_and_urls(file_id), 'zh'])

    # 数据结构：prog_data[norm_id][date_str] = {'length': 0, 'elements': []}
    prog_data = defaultdict(lambda: defaultdict(lambda: {'length': 0, 'elements': []}))
    valid_channels = set()
    today = datetime.now(TZ_UTC_PLUS_8).date()

    for prog in root.findall('programme'):
        file_id = prog.get('channel')
        norm_id = file_id_to_norm.get(file_id)
        if not norm_id: continue

        start_dt = parse_epg_datetime(prog.get('start'))
        stop_dt = parse_epg_datetime(prog.get('stop'))
        if not start_dt or not stop_dt: continue

        # 检查频道活跃状态
        if stop_dt.date() == today:
            valid_channels.add(norm_id)

        date_str = start_dt.strftime("%Y-%m-%d")

        # 重建 Programme 节点
        new_prog = ET.Element('programme', {
            'start': start_dt.strftime("%Y%m%d%H%M%S %z"),
            'stop': stop_dt.strftime("%Y%m%d%H%M%S %z"),
            'channel': norm_id
        })

        title_len = 0
        for title in prog.findall('title'):
            clean_title = clean_text_and_urls(title.text) or "精彩节目"
            title_len += len(clean_title)  # 4. 只计算 title 的长度
            lang = title.get('lang', 'zh')
            ET.SubElement(new_prog, 'title', {'lang': lang}).text = clean_title

        for desc in prog.findall('desc'):
            clean_desc = clean_text_and_urls(desc.text)
            if clean_desc:
                lang = desc.get('lang', 'zh')
                ET.SubElement(new_prog, 'desc', {'lang': lang}).text = clean_desc

        prog_data[norm_id][date_str]['length'] += title_len
        prog_data[norm_id][date_str]['elements'].append(new_prog)

    return channel_names, prog_data, valid_channels


def write_to_xml(channels_names, merged_progs, filename):
    os.makedirs('output', exist_ok=True)
    current_time = datetime.now(TZ_UTC_PLUS_8).strftime("%Y%m%d%H%M%S %z")
    root = ET.Element('tv', attrib={'date': current_time})

    for norm_id, dates_data in merged_progs.items():
        if not dates_data: continue

        # 写入 Channel 节点
        c_elem = ET.SubElement(root, 'channel', attrib={"id": norm_id})
        # 去重写入 display-name
        written_names = set()
        for name, lang in channels_names[norm_id]:
            if name not in written_names:
                ET.SubElement(c_elem, 'display-name', attrib={"lang": lang}).text = name
                written_names.add(name)

        # 写入 Programme 节点并记录日志 (Req 5)
        for date_str, data in sorted(dates_data.items()):
            logging.info(f"[{date_str}] 频道: {norm_id:<15} | 来源: {data['source']} (标题总字数: {data['length']})")
            for prog in data['elements']:
                root.append(prog)

    # 1. 提速：使用 ElementTree 原生 indent 替代缓慢的 minidom
    if hasattr(ET, 'indent'):
        ET.indent(root, space='\t', level=0)
    tree = ET.ElementTree(root)
    tree.write(filename, encoding='utf-8', xml_declaration=True)


def compress_to_gz(input_filename, output_filename):
    with open(input_filename, 'rb') as f_in:
        with gzip.open(output_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def get_urls():
    urls = []
    if not os.path.exists('config.txt'):
        print("未找到 config.txt 文件。")
        return urls
    with open('config.txt', 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    return urls


async def main():
    urls = get_urls()
    if not urls: return
    
    print("开始并发抓取 EPG 数据...")
    tasks = [fetch_epg(url) for url in urls]
    results = await tqdm_asyncio.gather(*tasks, desc="下载进度")
    
    all_channel_names = defaultdict(list)
    # merged_progs[norm_id][date_str] = {'length': 0, 'source': '', 'elements': []}
    merged_progs = defaultdict(lambda: defaultdict(lambda: {'length': -1, 'source': '', 'elements': []}))

    with tqdm(total=len(results), desc="解析并合并 EPG", unit="源") as pbar:
        for url, epg_content in results:
            if not epg_content:
                pbar.update(1)
                continue
                
            names, progs, valid_channels = parse_epg_content(epg_content, url)
            
            # 过滤掉今天没有节目结束的死频道
            for norm_id in valid_channels:
                # 汇总频道名称
                all_channel_names[norm_id].extend(names[norm_id])
                
                # 4. 合并逻辑：按天比对 title 总长度
                for date_str, data in progs[norm_id].items():
                    if data['length'] > merged_progs[norm_id][date_str]['length']:
                        merged_progs[norm_id][date_str] = {
                            'length': data['length'],
                            'source': url,
                            'elements': data['elements']
                        }
            pbar.update(1)

    print("正在写入 XML 文件并生成日志 (epg_source.log)...")
    write_to_xml(all_channel_names, merged_progs, 'output/epg.xml')
    print("正在压缩为 GZ 格式...")
    compress_to_gz('output/epg.xml', 'output/epg.gz')
    print("处理完成！输出位于 output/ 目录下。")

if __name__ == '__main__':
    asyncio.run(main())
