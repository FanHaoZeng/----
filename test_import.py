import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

def test_connection():
    """测试数据库连接"""
    try:
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database']
        )
        if connection.is_connected():
            print("数据库连接成功")
            return connection
    except Error as e:
        print(f"数据库连接失败: {e}")
        return None

def check_table_exists(connection):
    """检查表是否存在"""
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES LIKE 'dwd_xlyc_opl_weather_de'")
        result = cursor.fetchone()
        if result:
            print("表 dwd_xlyc_opl_weather_de 存在")
            return True
        else:
            print("表 dwd_xlyc_opl_weather_de 不存在")
            return False
    except Error as e:
        print(f"检查表时出错: {e}")
        return False

def count_records(connection):
    """统计记录数量"""
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM dwd_xlyc_opl_weather_de")
        count = cursor.fetchone()[0]
        print(f"总记录数: {count}")
        return count
    except Error as e:
        print(f"统计记录时出错: {e}")
        return 0

def count_by_city(connection):
    """按城市统计记录数量"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
            SELECT city, COUNT(*) as count 
            FROM dwd_xlyc_opl_weather_de 
            GROUP BY city 
            ORDER BY count DESC
        """)
        results = cursor.fetchall()
        print("各城市记录数:")
        for city, count in results:
            print(f"  - {city}: {count} 条记录")
    except Error as e:
        print(f"按城市统计时出错: {e}")

def show_sample_data(connection, limit=5):
    """显示样本数据"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            SELECT * FROM dwd_xlyc_opl_weather_de 
            ORDER BY date DESC 
            LIMIT {limit}
        """)
        results = cursor.fetchall()
        print(f"最新 {limit} 条记录:")
        for row in results:
            print(f"  - {row}")
    except Error as e:
        print(f"显示样本数据时出错: {e}")

def main():
    print("开始测试数据导入结果...")
    
    # 测试连接
    connection = test_connection()
    if connection is None:
        return
    
    try:
        # 检查表是否存在
        if not check_table_exists(connection):
            return
        
        # 统计记录数
        count_records(connection)
        
        # 按城市统计
        count_by_city(connection)
        
        # 显示样本数据
        show_sample_data(connection)
        
        print("\n测试完成！")
        
    except Error as e:
        print(f"测试过程中出错: {e}")
    
    finally:
        if connection.is_connected():
            connection.close()
            print("数据库连接已关闭")

if __name__ == "__main__":
    main() 