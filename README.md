# 天气数据导入MySQL工具

这个工具用于将CSV格式的天气数据导入到MySQL数据库中。

## 文件说明

- `import_weather_to_mysql.py`: 主要的导入脚本（逐行插入）
- `import_weather_to_mysql_batch.py`: 高效的批量导入脚本（推荐使用）
- `test_import.py`: 测试脚本，用于验证数据导入结果
- `config.py`: 数据库连接配置文件
- `*.csv`: 天气数据文件

## 使用步骤


### 1. 配置数据库连接

编辑 `config.py` 文件，修改数据库连接参数：

```python
DB_CONFIG = {
    'host': 'localhost',      # MySQL服务器地址
    'user': 'root',          # 数据库用户名
    'password': 'your_password', # 数据库密码
    'database': 'weather_db' # 数据库名称
}
```

### 2. 运行导入脚本

推荐使用批量导入版本（更快）：

```bash
python import_weather_to_mysql_batch.py
```

或者使用标准版本：

```bash
python import_weather_to_mysql.py
```

### 3. 验证数据导入

运行测试脚本验证数据是否正确导入：

```bash
python test_import.py
```

## 数据表结构

脚本会自动创建以下表结构：

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
