"""
天气数据完整流程脚本
直接调用现有函数，从获取数据到导入数据库
"""

import weather_2345 as fetch_weather
import import_weather_to_mysql as import_to_mysql
import os

def main():
    """
    先获取数据，再导入数据库
    """
    print("=== 天气数据完整流程 ===")
    print("步骤1: 获取天气数据")
    print("步骤2: 导入到数据库")
    print()
    
    # 步骤1: 获取数据
    # 从文件读取参数，支持批量处理多个城市和多组数据

    param_file = "./city.csv"
    param_list = []

    if not os.path.exists(param_file):
        print(f"未找到参数文件: {param_file}，请先创建该文件，格式如：city_name,year,month,month_count")
        exit(1)

    with open(param_file, "r", encoding="utf-8-sig") as f:
        lines = f.readlines()
        if not lines:
            print(f"参数文件 {param_file} 为空，请检查内容")
            exit(1)
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split(",")]
            if len(parts) != 4:
                print(f"参数格式错误: {line}，应为 city_name,year,month,month_count")
                continue
            city_name, year, month, month_count = parts
            param_list.append({
                "city_name": city_name,
                "year": int(year),
                "month": int(month),
                "month_count": int(month_count)
            })

    for param in param_list:
        print(f"开始获取 {param['city_name']} {param['year']}年{param['month']}月，连续{param['month_count']}个月的数据...")
        fetch_weather.fetch_multi_month_weather(
            city_name=param["city_name"],
            year=param["year"],
            month=param["month"],
            month_count=param["month_count"]
        )
    
    # 步骤2: 导入数据库
    print("\n开始导入数据库...")
    import_to_mysql.main()


if __name__ == "__main__":
    main() 