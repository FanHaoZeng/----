# 天气数据自动获取与导入MySQL工具

本项目实现了从2345天气网自动获取历史天气数据，并一键导入MySQL数据库的完整流程，支持单城市和批量城市处理。

## 主要文件说明

- `weather_2345.py`：天气数据爬取与处理脚本
- `import_weather_to_mysql.py`：CSV数据导入MySQL（逐行插入）
- `import_weather_to_mysql_batch.py`：CSV数据批量导入MySQL（推荐，速度更快）
- `weather_pipeline.py`：一键全流程脚本（推荐使用）
- `config.py`：数据库连接配置
- `city.csv`：批量城市及参数配置文件
- `test_import.py`：测试脚本，验证数据导入结果
- `data/`：存放原始和处理后的CSV数据文件

## 快速开始

### 1. 配置数据库连接

编辑 `config.py` 文件，修改数据库连接参数：

```python
DB_CONFIG = {
    'host': 'localhost',      # MySQL服务器地址
    'user': 'root',           # 数据库用户名
    'password': 'your_password', # 数据库密码
    'database': 'weather_db'  # 数据库名称
}
```

### 2. 一键全流程操作

推荐使用 `weather_pipeline.py`，自动完成数据获取、保存、导入和文件归档：

```bash
python weather_pipeline.py
```

准备 `city.csv` 文件，格式如下：

```csv
北京,2024,5,3
上海,2024,5,3
广州,2024,5,3
```

每行一个城市，year为年份，month为起始月份，month_count为连续月数。
获取从当前Year.Month开始，历史month_count个月的数据，（包含起始月份）

### 4. 仅导入现有CSV文件

如只需将已有CSV导入数据库，可直接运行：

```bash
python import_weather_to_mysql.py
# 或
python import_weather_to_mysql_batch.py
```

### 5. 验证数据导入

运行测试脚本验证数据是否正确导入：

```bash
python test_import.py
```

## 数据表结构

脚本会自动创建如下表结构：

```sql
CREATE TABLE `dwd_xlyc_opl_weather_de` (
  `city` text,
  `temperature_low` bigint(20) DEFAULT NULL,
  `temperature_high` bigint(20) DEFAULT NULL,
  `weather_start` bigint(20) DEFAULT NULL,
  `weather_end` bigint(20) DEFAULT NULL,
  `wind_level` bigint(20) DEFAULT NULL,
  `air_quality` bigint(20) DEFAULT NULL,
  `date` datetime DEFAULT NULL
)
```

## 注意事项
- 已导入的CSV文件会自动移动到 `data/imported/` 文件夹，避免重复导入。
- 如需自定义批量城市参数，请编辑 `city.csv` 文件。
