import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from xml.dom import minidom
from collections import defaultdict
from datetime import datetime, timezone, timedelta # 确保timezone和timedelta已导入
import re
import os
import gzip
from tqdm.asyncio import tqdm_asyncio # 异步进度条
from tqdm import tqdm # 同步进度条

# --- 简体中文转换函数占位符 ---
# 您需要提供此函数的实际实现。
# 它用于规范化中文频道名称/ID。
def transform2_zh_hans(text):
    if text is None:
        return ""
    # 示例：替换全角空格，转换为简体中文等。
    # 这是一个非常基础的占位符。
    text = text.replace('０', '0').replace('１', '1').replace('２', '2').replace('３', '3') \
               .replace('４', '4').replace('５', '5').replace('６', '6').replace('７', '7') \
               .replace('８', '8').replace('９', '9')
    # 如果需要，添加更高级的转换 (例如，使用 opencc-python-reimplemented)
    return text.strip()

# --- 异步获取EPG内容 ---
async def fetch_epg(url):
    # 稍微增加了连接器限制，根据需要调整
    connector = aiohttp.TCPConnector(limit=30, ssl=False, force_close=True)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
    }
    try:
        async with aiohttp.ClientSession(connector=connector, trust_env=True, headers=headers) as session:
            # 增加了超时设置
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                response.raise_for_status() # 对错误的HTTP状态码抛出异常
                return await response.text(encoding='utf-8')
    except aiohttp.ClientResponseError as e:
        print(f"请求 {url} 时发生HTTP错误: {e.status} {e.message}")
    except aiohttp.ClientError as e:
        print(f"请求 {url} 时发生客户端错误: {e}")
    except asyncio.TimeoutError:
        print(f"请求 {url} 超时。")
    except Exception as e:
        print(f"获取 {url} 时发生其他错误: {e}")
    return None

# --- 解析单个EPG文件的内容 ---
def parse_epg(epg_content):
    if not epg_content:
        return {}, defaultdict(list)
    try:
        parser = ET.XMLParser(encoding='UTF-8')
        root = ET.fromstring(epg_content, parser=parser)
    except ET.ParseError as e:
        print(f"解析XML时发生错误: {e}")
        # 如果需要，打印更多上下文进行调试
        # print(f"有问题的XML内容片段: {epg_content[:1000]}")
        return {}, defaultdict(list) # 如果解析失败则返回空

    parsed_channels = {} # 存储 {原始转换后ID: 转换并清理后的显示名称}
    parsed_programmes = defaultdict(list) # 存储 {原始转换后ID: [节目元素列表]}

    for channel_tag in root.findall('channel'):
        channel_id_original = channel_tag.get('id')
        if channel_id_original is None:
            print("警告: 发现没有 'id' 属性的频道标签。已跳过。")
            continue
        
        channel_id_transformed = transform2_zh_hans(channel_id_original)
        
        display_name_tag = channel_tag.find('display-name')
        display_name_text = ""
        if display_name_tag is not None and display_name_tag.text is not None:
            display_name_text = transform2_zh_hans(display_name_tag.text)
        else:
            # 如果没有display-name，则使用channel_id作为显示名称的后备
            display_name_text = channel_id_transformed
            print(f"警告: 频道 '{channel_id_transformed}' 没有display-name。将使用ID作为名称。")

        if not channel_id_transformed: # 如果ID转换后为空则跳过
            print(f"警告: 频道ID '{channel_id_original}' 转换后为空。已跳过。")
            continue
            
        parsed_channels[channel_id_transformed] = display_name_text

    for programme_tag in root.findall('programme'):
        prog_channel_id_original = programme_tag.get('channel')
        if prog_channel_id_original is None:
            print("警告: 发现没有 'channel' 属性的节目标签。已跳过。")
            continue

        prog_channel_id_transformed = transform2_zh_hans(prog_channel_id_original)

        if not prog_channel_id_transformed:
            print(f"警告: 节目的频道ID '{prog_channel_id_original}' 转换后为空。已跳过该节目。")
            continue
        
        # 如果此节目的频道ID未在<channel>标签中定义，
        # 我们可能仍希望处理它，使用其ID作为其假定名称。
        if prog_channel_id_transformed not in parsed_channels:
            parsed_channels[prog_channel_id_transformed] = prog_channel_id_transformed
            # print(f"信息: 节目引用的频道 '{prog_channel_id_transformed}' 未被明确定义。将使用ID作为名称。")

        try:
            start_str = programme_tag.get('start')
            stop_str = programme_tag.get('stop')
            if not start_str or not stop_str:
                print(f"警告: 频道 '{prog_channel_id_transformed}' 的节目缺少开始/结束时间。已跳过。")
                continue

            # 确保日期时间解析的健壮性
            # re.sub用于删除日期时间字符串中意外的空白字符
            # 假设输入的时间字符串包含时区信息 (如 +0800 或 Z)
            channel_start = datetime.strptime(
                re.sub(r'\s+', '', start_str), "%Y%m%d%H%M%S%z")
            channel_stop = datetime.strptime(
                re.sub(r'\s+', '', stop_str), "%Y%m%d%H%M%S%z")
        except ValueError as ve:
            print(f"解析频道 '{prog_channel_id_transformed}' 的节目日期时出错: {ve}。开始: '{start_str}', 结束: '{stop_str}'。已跳过。")
            continue
            
        title_tag = programme_tag.find('title')
        channel_text = ""
        if title_tag is not None and title_tag.text is not None:
            channel_text = transform2_zh_hans(title_tag.text)
        else:
            # print(f"警告: 频道 '{prog_channel_id_transformed}' 在 {channel_start} 的节目没有标题。将使用空标题。")
            pass # 如果需要，允许空标题的节目，或者跳过

        # 创建新的节目元素。这确保我们不会修改原始树。
        # 我们将存储这些元素，稍后将它们附加到新的主XML树中。
        # 输出时统一使用UTC (+0000)
        new_prog_elem = ET.Element('programme', attrib={
            "channel": prog_channel_id_transformed, # 使用此EPG上下文中的转换后ID
            "start": channel_start.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S +0000"), # 输出为UTC
            "stop": channel_stop.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S +0000")    # 输出为UTC
        })
        title_elem = ET.SubElement(new_prog_elem, 'title', attrib={"lang": "zh"})
        title_elem.text = channel_text
        
        # 可选：如果存在，添加其他节目子元素 (desc, category, 等)
        desc_tag = programme_tag.find('desc')
        if desc_tag is not None and desc_tag.text:
            desc_elem = ET.SubElement(new_prog_elem, 'desc', attrib={"lang": "zh"})
            desc_elem.text = transform2_zh_hans(desc_tag.text)

        parsed_programmes[prog_channel_id_transformed].append(new_prog_elem)

    return parsed_channels, parsed_programmes

# --- 将合并后的EPG数据写入XML文件 ---
def write_to_xml(final_channel_names, final_programmes, filename):
    if not os.path.exists('output'):
        os.makedirs('output')
        print("创建输出目录: output")
    
    # 为<tv>标签的'date'属性使用当前的UTC时间
    current_time_utc = datetime.now(timezone.utc)
    root = ET.Element('tv', attrib={'date': current_time_utc.strftime("%Y%m%d%H%M%S +0000")})

    # 对频道名称进行排序以获得一致的输出顺序 (可选但推荐)
    sorted_channel_names = sorted(list(final_channel_names))

    print(f"准备写入 {len(sorted_channel_names)} 个频道到XML...")
    for master_channel_name in tqdm(sorted_channel_names, desc="写入频道数据", unit="频道"):
        channel_elem = ET.SubElement(root, 'channel', attrib={"id": master_channel_name})
        display_name_elem = ET.SubElement(channel_elem, 'display-name', attrib={"lang": "zh"})
        display_name_elem.text = master_channel_name # ID和显示名称使用相同的统一名称

        # 按开始时间对节目进行排序 (可选但推荐)
        # prog_elem.get('start') 的格式为 "%Y%m%d%H%M%S +0000"
        sorted_progs = sorted(
            final_programmes.get(master_channel_name, []),
            key=lambda p: p.get('start')
        )

        for prog_elem in sorted_progs:
            # 关键：确保节目的 'channel' 属性与 master_channel_name 匹配
            # 这很重要，因为 prog_elem 是用其原始EPG的频道ID创建的。
            prog_elem.set('channel', master_channel_name)
            root.append(prog_elem) # 追加到主根元素，而不是channel_elem

    rough_string = ET.tostring(root, encoding='utf-8', xml_declaration=True)
    reparsed = minidom.parseString(rough_string)
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(reparsed.toprettyxml(indent='  ', newl='\n')) # 使用2个空格缩进
        print(f"成功将合并后的EPG写入到 {filename}")
    except IOError as e:
        print(f"写入文件 {filename} 时发生错误: {e}")


# --- 压缩文件到 .gz ---
def compress_to_gz(input_filename, output_filename):
    try:
        with open(input_filename, 'rb') as f_in, gzip.open(output_filename, 'wb') as f_out:
            f_out.write(f_in.read())
        print(f"成功将 {input_filename} 压缩到 {output_filename}")
    except Exception as e:
        print(f"压缩文件时发生错误: {e}")

# --- 从config.txt读取URL ---
def get_urls():
    urls = []
    config_file = "config.txt"
    if not os.path.exists(config_file):
        print(f"错误: 配置文件 '{config_file}' 未找到。请创建该文件并在其中包含EPG URL。")
        return urls
        
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'): # 忽略空行和注释行
                    urls.append(line)
        if not urls:
            print(f"警告: '{config_file}' 文件中没有找到有效的URL。")
        else:
            print(f"从 '{config_file}' 文件中加载了 {len(urls)} 个URL。")
    except IOError as e:
        print(f"读取配置文件 '{config_file}' 时发生错误: {e}")
    return urls

# --- 主程序逻辑 ---
async def main():
    urls = get_urls()
    if not urls:
        print("没有提供EPG URL。正在退出。")
        return

    tasks = [fetch_epg(url) for url in urls]
    print(f"正在从 {len(urls)} 个源获取EPG数据...")
    # 使用tqdm_asyncio实现异步任务的进度条
    epg_contents = await tqdm_asyncio.gather(*tasks, desc="获取URL", unit="个URL")

    master_channel_display_names = set() # 存储所有唯一的、规范化后的频道显示名称
    master_programmes = defaultdict(list) # 键是规范化的频道显示名称，值是该频道的节目XML元素列表
    
    # 用于避免因来源不同或重叠而导致的同一频道重复节目
    # 键: master_channel_display_name, 值: (开始时间字符串, 结束时间字符串, 标题字符串) 的集合
    added_program_signatures = defaultdict(set)

    print("正在解析和合并EPG数据...")
    # 使用tqdm实现同步循环的进度条
    for i, epg_content in enumerate(tqdm(epg_contents, desc="处理EPG文件", unit="个文件")):
        if epg_content is None:
            print(f"跳过第 {i+1} 个EPG源，因获取时发生错误。")
            continue
        
        # parsed_channels_from_file: {原始转换后ID: 转换并清理后的显示名称}
        # parsed_programmes_from_file: {原始转换后ID: [节目元素列表]}
        parsed_channels_from_file, parsed_programmes_from_file = parse_epg(epg_content)

        if not parsed_channels_from_file and not parsed_programmes_from_file:
            print(f"跳过第 {i+1} 个EPG源，因为它在解析后没有产生任何频道或节目。")
            continue

        # 遍历当前EPG文件中定义的频道
        for original_channel_id, display_name_from_file in parsed_channels_from_file.items():
            # 合并的关键将是 display_name，并进一步清理。
            # display_name_from_file 已经被转换过，并且在display-name标签缺失时使用ID作为后备。
            master_key_channel_name = display_name_from_file.replace(' ', '') # 移除空格以获得更干净的键
            
            if not master_key_channel_name: # 如果显示名称为空，则跳过
                # print(f"警告: 原始ID为 '{original_channel_id}' 的频道处理后显示名称为空。已跳过。")
                continue

            master_channel_display_names.add(master_key_channel_name)

            # 现在，将此频道的节目 (在parsed_programmes_from_file中以original_channel_id为键)
            # 添加到主列表，使用 master_key_channel_name。
            if original_channel_id in parsed_programmes_from_file:
                for prog_elem in parsed_programmes_from_file[original_channel_id]:
                    prog_start_str = prog_elem.get('start') # 已经是 YYYYMMDDHHMMSS+ZZZZ 格式
                    prog_stop_str = prog_elem.get('stop')   # 已经是 YYYYMMDDHHMMSS+ZZZZ 格式
                    
                    title_elem = prog_elem.find('title')
                    prog_title_str = title_elem.text if title_elem is not None and title_elem.text is not None else ""
                    
                    # 用于去重的签名
                    signature = (prog_start_str, prog_stop_str, prog_title_str)

                    if signature not in added_program_signatures[master_key_channel_name]:
                        master_programmes[master_key_channel_name].append(prog_elem)
                        added_program_signatures[master_key_channel_name].add(signature)
                    # else:
                        # print(f"调试信息: 为频道 {master_key_channel_name} 跳过了重复节目: {prog_title_str} 开始于 {prog_start_str}")

    if not master_channel_display_names:
        print("所有EPG源均未处理得到任何频道。输出将为空。")
    
    print(f"共识别出 {len(master_channel_display_names)} 个唯一频道名称。")
    total_progs = sum(len(progs) for progs in master_programmes.values())
    print(f"共整理出 {total_progs} 个唯一节目。")

    print("正在将合并后的EPG写入XML...")
    output_xml_file = 'output/epg.xml'
    output_gz_file = 'output/epg.gz'
    
    write_to_xml(master_channel_display_names, master_programmes, output_xml_file)
    if os.path.exists(output_xml_file): # 只有在XML文件成功创建后才进行压缩
        compress_to_gz(output_xml_file, output_gz_file)
    else:
        print(f"XML文件 {output_xml_file} 未创建，跳过压缩步骤。")
    
    print("EPG处理完成。")

if __name__ == '__main__':
    asyncio.run(main())
