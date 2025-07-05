# feature_engineering.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# 1. 初始化 Spark Session
# ==============================================================================
spark = SparkSession.builder \
    .appName("ML_Feature_Engineering") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")

db_name = "flight_dw"
spark.sql(f"USE {db_name}")
print(f"INFO: 已切换到数据库: {db_name}")


# ==============================================================================
# 2. 加载核心数据表
# ==============================================================================
print("INFO: 正在加载 fact_flight_ticket 和 dim_date...")
# 事实表包含了我们的核心交易记录
fact_df = spark.table("fact_flight_ticket")
# 日期维度表可以为我们提供丰富的日期特征
dim_date_df = spark.table("dim_date")


# ==============================================================================
# 3. 特征提取与丰富
# ==============================================================================
print("INFO: 正在关联维度表，提取时间特征...")
# 注意：我们使用 flight_date_fk 进行关联，因为季节性是由飞行日决定的，而不是搜索日
model_df = fact_df.join(
    dim_date_df,
    fact_df.flight_date_fk == dim_date_df.date_fk
).select(
    fact_df.leg_id,                   # 行程的唯一ID，用于定位一个特定的航班
    fact_df.total_fare,               # 回归模型的 Label
    fact_df.search_to_flight_days,    # 核心特征：提前预订天数
    fact_df.departure_airport_fk,     # 核心特征：出发机场
    fact_df.arrival_airport_fk,       # 核心特征：到达机场
    fact_df.airline_fk,               # 核心特征：航司
    fact_df.aircraft_fk,              # 附加特征：机型
    # --- 从日期维度表获取的时间特征 ---
    dim_date_df.day_of_week.alias("flight_day_of_week"), # 航班是周几
    dim_date_df.month.alias("flight_month"),             # 航班所在月份（季节性）
    dim_date_df.week_of_year.alias("flight_week_of_year"), # 航班所在年份的周数
)



# 4. 【魔法发生的地方】创建“最佳购买时机”的Label

print("INFO: 正在使用窗口函数计算每个行程的历史最低价...")
# 定义一个“窗口”，它会按 leg_id 对数据进行分组
# 这意味着，所有关于同一个航班（比如 MU583 在 2025-10-01 的航班）的票价记录都会被放在一起计算
window_spec = Window.partitionBy("leg_id")

# 使用窗口函数 over(window_spec)，为每一行数据都计算出它所属的那个航班的【历史最低价】
model_df_with_min_price = model_df.withColumn(
    "min_fare_for_leg", F.min("total_fare").over(window_spec)
)

# 应用我们的法则
model_df_final = model_df_with_min_price.withColumn(
    "is_best_time_to_buy", # 这就是我们分类模型的 Label
    F.when(
        F.col("total_fare") <= (F.col("min_fare_for_leg") * 1.05),
        1  # 如果当前价格在历史最低价的5%范围内，标记为 1 (是)
    ).otherwise(0) # 否则，标记为 0 (否)
)



# ==============================================================================
# 5. 保存最终的训练数据表
# ==============================================================================
TARGET_TABLE = "ml_training_data"
print(f"INFO: 开始将最终的训练数据写入到新表 `{TARGET_TABLE}` 中...")

# 删除了 min_fare_for_leg 这一列，因为它只是我们创建label的中间产物，不应作为模型的特征
model_df_final.drop("min_fare_for_leg").write.mode("overwrite").saveAsTable(TARGET_TABLE)


# ==============================================================================
# 6. 结束任务
# ==============================================================================
spark.stop()
