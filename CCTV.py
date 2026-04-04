import requests
from datetime import datetime, timedelta, timezone
import xml.sax.saxutils as saxutils
import concurrent.futures
import json

# 频道列表
CHANNELS =[
    'cctv1', 'cctv2', 'cctv3', 'cctv4', 'cctveurope', 'cctvamerica', 'cctv5',
    'cctv5plus', 'cctv6', 'cctv7', 'cctv8', 'cctvjilu', 'cctv10', 'cctv11',
    'cctv12', 'cctv13', 'cctvchild', 'cctv15', 'cctv16', 'cctv17', 'cctv4k',
    'cctv8k', 'shijiedili', 'dianshigouwu', 'guofang', 'taiqiu', 'jingpin', 
    'shishang', 'cctvyule', 'hjjc', 'cctvxiqu', 'xinkedongman', 'cctvqixiang', 
    'zhinan', 'diyijuchang', 'cctvlaogushi', 'fyjc', 'cctvfyzq', 'fyyy', 'cctvgaowang'
]

# 设置时区为北京时间 (UTC+8)
TZ_BJ = timezone(timedelta(hours=8))

def get_dates():
    """获取 7天前 + 今天 + 7天后 的日期列表 (YYYYMMDD)"""
    today = datetime.now(TZ_BJ)
    return[(today + timedelta(days=i)).strftime('%Y%m%d') for i in range(-7, 8)]

def fetch_epg(channel_id, date_str, session):
    """抓取单个频道某天的 EPG 数据"""
    url = f"https://api.cntv.cn/epg/epginfo?c={channel_id}&d={date_str}"
    try:
        # 设置超时时间，防止卡死
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return channel_id, date_str, data
    except Exception as e:
        print(f"[-] 获取失败 {channel_id} ({date_str}): {e}")
        return channel_id, date_str, None

def format_ts(ts):
    """将 Unix 时间戳转换为 XMLTV 标准时间格式 (YYYYMMDDHHMMSS +0800)"""
    dt = datetime.fromtimestamp(ts, tz=TZ_BJ)
    return dt.strftime('%Y%m%d%H%M%S +0800')

def main():
    dates = get_dates()
    channel_names = {}
    programs = []
    
    print(f"[*] 开始抓取 {len(CHANNELS)} 个频道，共 {len(dates)} 天的 EPG 数据...")
    
    # 使用线程池并发请求，加快抓取速度 (最大线程数设为 20)
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        with requests.Session() as session:
            futures =[]
            # 提交所有任务
            for channel_id in CHANNELS:
                for date_str in dates:
                    futures.append(executor.submit(fetch_epg, channel_id, date_str, session))
            
            # 收集结果
            for future in concurrent.futures.as_completed(futures):
                channel_id, date_str, data = future.result()
                if data and channel_id in data:
                    channel_data = data[channel_id]
                    
                    # 提取并保存 channelName (如果存在)
                    if 'channelName' in channel_data and channel_id not in channel_names:
                        channel_names[channel_id] = channel_data['channelName']
                        
                    # 提取节目单
                    if 'program' in channel_data:
                        for prog in channel_data['program']:
                            # 确保包含必要的字段
                            if 'st' in prog and 'et' in prog and 't' in prog:
                                programs.append({
                                    'channel': channel_id,
                                    'start': prog['st'],
                                    'stop': prog['et'],
                                    'title': prog['t']
                                })

    # 对节目单进行排序：按频道ID和开始时间排序，使生成的 XML 更有条理
    programs.sort(key=lambda x: (x['channel'], x['start']))

    print("[*] 数据抓取完毕，正在生成 epg.xml 文件...")
    
    # 生成 XMLTV 格式文件
    with open("CCTV.xml", "w", encoding="utf-8") as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<tv generator-info-name="CNTV EPG Generator">\n')
        
        # 写入 <channel> 节点
        for channel_id in CHANNELS:
            # 如果接口中没有返回 channelName，则默认使用大写的 channel_id
            name = channel_names.get(channel_id, channel_id.upper())
            name_esc = saxutils.escape(name)
            f.write(f'  <channel id="{channel_id}">\n')
            f.write(f'    <display-name>{name_esc}</display-name>\n')
            f.write(f'  </channel>\n')
            
        # 写入 <programme> 节点
        for prog in programs:
            title_esc = saxutils.escape(prog['title'])
            start_str = format_ts(prog['start'])
            stop_str = format_ts(prog['stop'])
            f.write(f'  <programme start="{start_str}" stop="{stop_str}" channel="{prog["channel"]}">\n')
            f.write(f'    <title>{title_esc}</title>\n')
            f.write(f'  </programme>\n')
            
        f.write('</tv>\n')
        
    print("[+] 成功！EPG 文件已保存为 CCTV.xml")

if __name__ == "__main__":
    main()
