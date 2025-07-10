import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import urllib.parse
import os

def search_city_id(city_name):
    """
    通过2345天气网接口根据城市名搜索城市ID，默认选择第一个结果
    :param city_name: 城市名（如“北京”）
    :return: (城市ID, 城市显示名) 或 None
    """
    q = urllib.parse.quote(city_name)
    url = f"https://tianqi.2345.com/tpc/searchCity.php?q={q}"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data or "res" not in data or not data["res"]:
            print("未找到相关城市")
            return None
        # 默认选择第一个结果
        item = data["res"][0]
        # 从href中提取城市ID
        href = item.get("href", "")
        # 匹配href末尾的数字ID
        import re
        match = re.search(r'/(\d+)$', href)
        if not match:
            print("未能从href中提取城市ID")
            return None
        city_id = match.group(1)
        # 提取城市显示名
        # text 可能为 '北京市 - <span>北京</span>'，只取第一个汉字部分
        text = item.get("text", "")
        city_name = text.split('-')[0].strip() if '-' in text else text.strip()
        print(f"已自动选择：{city_name} (ID: {city_id})")
        return city_id, city_name
    except Exception as e:
        print(f"城市ID查询失败: {e}")
        return None

def fetch_2345_weather(city_name, area_id, year, month):
    """
    爬取2345天气网指定地区、年份、月份的历史天气数据
    :param area_id: 地区ID（如南京为54511）
    :param year: 年份（如2025）
    :param month: 月份（1-12）
    :return: 天气数据列表
    """
    url = f"https://tianqi.2345.com/Pc/GetHistory?areaInfo%5BareaId%5D={area_id}&areaInfo%5BareaType%5D=2&date%5Byear%5D={year}&date%5Bmonth%5D={month}"
    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "referer": f"https://tianqi.2345.com/wea_history/{area_id}.htm",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
        "x-requested-with": "XMLHttpRequest"
    }
    
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    html = data.get("data", "")
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="history-table")
    if not table:
        print("未找到历史天气表格")
        return []
    from bs4 import Tag
    if not isinstance(table, Tag):
        print("解析到的table不是有效的HTML标签")
        return []
    rows = table.find_all("tr")[1:]  # 跳过表头
    weather_list_raw = []
    weather_list = []
    # 天气类型映射字典
    weather_dict = {
        '晴': 1, '中雨': 2, '雾': 5, '小雨': 3, '大雨': 4, '阴': 6, '多云': 7, '雨夹雪': 8, '雷阵雨': 9, '阵雨': 9,
        '暴雨': 10, '大暴雨': 11, '小雪': 12, '中雪': 13, '大雪': 14, '晴间多云': 1, '少云': 1, '阵雪': 12, '雨雪天气': 8, '暴雪': 14,
        '扬沙': 13, '浮尘': 3
    }
    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue
        date_text = cols[0].get_text(strip=True)
        # 日期格式如 2025-04-01 周二，提取前10位
        date_str = date_text[:10]
        # 转换为datetime格式
        try:
            date = pd.to_datetime(date_str)
        except Exception:
            date = None

        # 最高温、最低温
        max_temp = re.sub(r"[^\d\-]", "", cols[1].get_text(strip=True))
        min_temp = re.sub(r"[^\d\-]", "", cols[2].get_text(strip=True))
        try:
            temperature_high = int(max_temp)
        except Exception:
            temperature_high = None
        try:
            temperature_low = int(min_temp)
        except Exception:
            temperature_low = None

        # 天气状况，可能为“多云~晴”或“晴”
        weather_condition = cols[3].get_text(strip=True)
        # 拆分天气状况，优先用“~”分割，没有则start和end一样
        if "~" in weather_condition:
            weather_start_str, weather_end_str = weather_condition.split("~", 1)
        else:
            weather_start_str = weather_condition
            weather_end_str = weather_condition

        # 天气类型映射
        weather_start = weather_dict.get(weather_start_str.strip(), None)
        weather_end = weather_dict.get(weather_end_str.strip(), None)

        # 风力风向
        wind = cols[4].get_text(strip=True)
        # 风力提取
        wind_level = None
        wind_level_match = re.search(r"(\d+)", wind)
        if wind_level_match:
            wind_level = int(wind_level_match.group(1))

        # AQI数值提取
        aqi_text = cols[5].get_text(strip=True)
        aqi_match = re.match(r"(\d+)", aqi_text)
        air_quality = int(aqi_match.group(1)) if aqi_match else None

        # 额外保存映射前的天气状况原始数据，并和其他数据一起保存到csv
        weather_start_raw = weather_start_str.strip()
        weather_end_raw = weather_end_str.strip()

        weather_list_raw.append({
            "city": city_name,
            "temperature_low": temperature_low,
            "temperature_high": temperature_high,
            "weather_start_raw": weather_start_raw,
            "weather_end_raw": weather_end_raw,
            "wind_level": wind_level,
            "air_quality": air_quality,
            "date": date
        })
        weather_list.append({
            "city": city_name,
            "temperature_low": temperature_low,
            "temperature_high": temperature_high,
            "weather_start": weather_start,
            "weather_end": weather_end,
            "wind_level": wind_level,
            "air_quality": air_quality,
            "date": date
        })
    return weather_list, weather_list_raw

def fetch_multi_month_weather(city_name, year, month, month_count):
    """
    获取多个月份的天气数据并保存为csv文件

    参数:
        city_name (str): 城市名称，如“北京”
        year (int): 年份，如2025
        month (int): 月份，1-12
        month_count (int): 需要获取的月数（如3，表示当前月及前2个月）
    """
    if not city_name or not isinstance(city_name, str):
        print("城市名不能为空")
        exit(1)
    city_info = search_city_id(city_name)
    if not city_info:
        print("未能获取城市ID，程序退出。")
        exit(1)
    area_id, area_name = city_info

    # 检查年份、月份和月数
    try:
        year = int(year)
        month = int(month)
        month_count = int(month_count)
        if not (1 <= month <= 12):
            print("月份必须在1-12之间")
            exit(1)
        if month_count < 1:
            print("月数必须大于等于1")
            exit(1)
    except Exception:
        print("年份、月份或月数输入有误")
        exit(1)

    # 计算需要获取的所有年月
    all_year_month = []
    cur_year, cur_month = year, month
    for i in range(month_count):
        all_year_month.append((cur_year, cur_month))
        # 递减月份
        cur_month -= 1
        if cur_month == 0:
            cur_month = 12
            cur_year -= 1

    all_weather_data = []
    all_weather_data_raw = []
    for y, m in all_year_month:
        print(f"正在获取 {area_name} {y}年{m}月 的数据...")
        weather_data, weather_data_raw = fetch_2345_weather(city_name, area_id, y, m)
        if weather_data:
            all_weather_data.extend(weather_data)
            all_weather_data_raw.extend(weather_data_raw)
        else:
            print(f"{y}年{m}月未获取到数据")

    if all_weather_data:
        df = pd.DataFrame(all_weather_data)
        print(df)
        # 保存为csv
        os.makedirs("./data", exist_ok=True)
        ym_range = f"{all_year_month[-1][0]}_{all_year_month[-1][1]:02d}_to_{all_year_month[0][0]}_{all_year_month[0][1]:02d}"
        df.to_csv(f"./data/{area_name}_2345历史天气_{ym_range}.csv", index=False, encoding="utf-8-sig")
        print(f"已保存为 {area_name}_2345历史天气_{ym_range}.csv")
    else:
        print("未获取到任何数据")

    # 同时保存原始数据
    if all_weather_data_raw:
        df_raw = pd.DataFrame(all_weather_data_raw)
        print(df_raw)
        # 保存为csv
        os.makedirs("./data/raw", exist_ok=True)
        ym_range = f"{all_year_month[-1][0]}_{all_year_month[-1][1]:02d}_to_{all_year_month[0][0]}_{all_year_month[0][1]:02d}"
        df_raw.to_csv(f"./data/raw/{area_name}_2345历史天气_raw_{ym_range}.csv", index=False, encoding="utf-8-sig")
        print(f"已保存为 {area_name}_2345历史天气_raw_{ym_range}.csv")
    else:
        print("未获取到任何原始数据")
