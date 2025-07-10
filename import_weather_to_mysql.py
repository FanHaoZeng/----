import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import glob
import shutil
from config import DB_CONFIG

def create_connection(host, user, password, database):
    """创建MySQL数据库连接"""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print(f"成功连接到MySQL数据库: {database}")
            return connection
    except Error as e:
        print(f"连接MySQL数据库时出错: {e}")
        return None

def create_table(connection):
    """创建天气数据表"""
    try:
        cursor = connection.cursor()
        
        # 创建表的SQL语句
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `dwd_xlyc_opl_weather_de` (
          `city` text,
          `temperature_low` bigint(20) DEFAULT NULL,
          `temperature_high` bigint(20) DEFAULT NULL,
          `weather_start` bigint(20) DEFAULT NULL,
          `weather_end` bigint(20) DEFAULT NULL,
          `wind_level` bigint(20) DEFAULT NULL,
          `air_quality` bigint(20) DEFAULT NULL,
          `date` datetime DEFAULT NULL
        )
        """
        
        cursor.execute(create_table_query)
        connection.commit()
        print("表 dwd_xlyc_opl_weather_de 创建成功或已存在")
        
    except Error as e:
        print(f"创建表时出错: {e}")

def import_csv_to_mysql(connection, csv_file):
    """将CSV文件数据导入到MySQL表"""
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        
        # 处理空值
        df = df.fillna('NULL')
        
        # 准备插入语句
        insert_query = """
        INSERT INTO dwd_xlyc_opl_weather_de 
        (city, temperature_low, temperature_high, weather_start, weather_end, wind_level, air_quality, date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor = connection.cursor()
        
        # 逐行插入数据
        for index, row in df.iterrows():
            # 处理数值类型，将'NULL'字符串转换为None
            values = []
            for value in row.values:
                if value == 'NULL' or pd.isna(value):
                    values.append(None)
                else:
                    values.append(value)
            
            cursor.execute(insert_query, values)
        
        connection.commit()
        print(f"成功导入 {len(df)} 条记录从文件: {os.path.basename(csv_file)}")
        
    except Error as e:
        print(f"导入数据时出错 {csv_file}: {e}")
        connection.rollback()

def main():
    # 从配置文件获取数据库连接配置
    host = DB_CONFIG['host']
    user = DB_CONFIG['user']
    password = DB_CONFIG['password']
    database = DB_CONFIG['database']
    
    # 创建数据库连接
    connection = create_connection(host, user, password, database)
    
    if connection is None:
        print("无法连接到数据库，请检查连接参数")
        return
    
    try:
        # 创建表
        create_table(connection)
        
        # 查找所有处理过的CSV文件（不包含raw文件）
        csv_files = glob.glob("./data/*_2345历史天气_*.csv")
        csv_files = [f for f in csv_files if "raw" not in f]
        
        print(f"找到 {len(csv_files)} 个CSV文件:")
        for file in csv_files:
            print(f"  - {file}")
        
        # 创建imported文件夹（如果不存在）
        imported_dir = "./data/imported"
        if not os.path.exists(imported_dir):
            os.makedirs(imported_dir)
            print(f"创建文件夹: {imported_dir}")
        
        # 导入每个文件的数据
        for csv_file in csv_files:
            print(f"\n正在处理文件: {csv_file}")
            import_csv_to_mysql(connection, csv_file)
            
            # 导入成功后，移动文件到imported文件夹
            try:
                filename = os.path.basename(csv_file)
                destination = os.path.join(imported_dir, filename)
                shutil.move(csv_file, destination)
                print(f"文件已移动到: {destination}")
            except Exception as e:
                print(f"移动文件时出错 {csv_file}: {e}")
        
        print("\n所有数据导入完成！")
        
    except Error as e:
        print(f"执行过程中出错: {e}")
    
    finally:
        if connection.is_connected():
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main() 