"""
天气数据完整流程脚本
直接调用现有函数，从获取数据到导入数据库
"""

import weather_2345 as fetch_weather
import import_weather_to_mysql as import_to_mysql


def main():
    """
    先获取数据，再导入数据库
    """
    print("=== 天气数据完整流程 ===")
    print("步骤1: 获取天气数据")
    print("步骤2: 导入到数据库")
    print()
    
    # 步骤1: 获取数据
    print("开始获取天气数据...")
    fetch_weather.fetch_multi_month_weather()
    
    # 步骤2: 导入数据库
    print("\n开始导入数据库...")
    import_to_mysql.main()


if __name__ == "__main__":
    main() 