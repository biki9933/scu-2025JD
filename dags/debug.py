# debug_nulls.py
# 一个专门用来诊断训练数据中空值、NaN或其他无效值的脚本

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Debug_Training_Data_Nulls") \
    .enableHiveSupport() \
    .getOrCreate()

db_name = "flight_dw"
spark.sql(f"USE {db_name}")

print("INFO: 开始加载 ml_training_data 表进行诊断...")
df = spark.table("ml_training_data")
df.cache() # 将数据加载到内存，加快后续的多次计算

total_rows = df.count()
print(f"INFO: 表中总行数: {total_rows}")
print("==================================================")
print("INFO: 开始逐列检查特征中的无效值...")
print("==================================================")


# --- 定义我们要检查的特征列 ---
numerical_cols = ["search_to_flight_days", "flight_week_of_year"]
categorical_cols = [
    "departure_airport_fk", "arrival_airport_fk", "airline_fk",
    "aircraft_fk", "flight_day_of_week", "flight_month"
]
all_feature_cols = numerical_cols + categorical_cols

# --- 循环检查每一列 ---
for col_name in all_feature_cols:
    print(f"--- 正在诊断列: [{col_name}] ---")

    # 1. 检查标准的 NULL 值
    null_count = df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  [发现问题!] 此列包含 {null_count} 个 NULL 值。")
    else:
        print(f"  [OK] 此列不包含 NULL 值。")

    # 2. 对于数值类型的列，额外检查 NaN (Not a Number)
    if col_name in numerical_cols:
        nan_count = df.filter(F.isnan(F.col(col_name))).count()
        if nan_count > 0:
            print(f"  [发现问题!] 此列包含 {nan_count} 个 NaN 值。")
        else:
            print(f"  [OK] 此列不包含 NaN 值。")

    # 3. 检查数据中是否包含字符串 "null" 或 "no"
    # 我们先将列转为字符串再进行比较，以防原始类型是数字
    str_null_count = df.filter(F.upper(F.trim(F.col(col_name).cast("string"))) == "NULL").count()
    if str_null_count > 0:
        print(f"  [发现问题!] 此列包含 {str_null_count} 个值为 'NULL' 的字符串。")

    str_no_count = df.filter(F.upper(F.trim(F.col(col_name).cast("string"))) == "NO").count()
    if str_no_count > 0:
        print(f"  [发现问题!] 此列包含 {str_no_count} 个值为 'NO' 的字符串。")

    print("\n")

print("==================================================")
print("INFO: 诊断完成。")
print("==================================================")

df.unpersist()
spark.stop()