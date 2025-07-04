from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 初始化 Spark Session
spark = SparkSession.builder \
    .appName("FlightML_FeatureEngineering") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")
spark.sql("USE flight_dw")
print("INFO: 已切换到数据库: flight_dw")

# 加载已经创建好的事实表和维度表
try:
    fact_table = spark.table("fact_flight_ticket")
    dim_date = spark.table("dim_date")
    dim_airport = spark.table("dim_airport")
    dim_airline = spark.table("dim_airline")
    print("SUCCESS: 事实表与维度表加载成功！")
except Exception as e:
    print(f"ERROR: 加载数据表失败: {e}")
    spark.stop()
    exit(1)
# 为了方便，给维度表起别名
dim_date_flight = dim_date.alias("flight_dt")
dim_date_search = dim_date.alias("search_dt")

# 关联事实表与维度表
base_df = fact_table \
    .join(dim_date_flight, fact_table.flight_date_fk == dim_date_flight.date_fk, "inner") \
    .join(dim_date_search, fact_table.search_date_fk == dim_date_search.date_fk, "inner")

print("INFO: 事实表与日期维度表关联完成。")

# 创建特征
# search_to_flight_days 是在ETL脚本中创建的
# total_fare 也是ETL脚本中创建的数值型票价
features_df = base_df.select(
    # 1. 价格标签 (用于回归模型)
    F.col("total_fare"),

    # 2. 时间相关特征
    F.col("search_to_flight_days"),
    F.col("flight_dt.day_of_week").alias("flight_day_of_week"),
    F.col("flight_dt.month").alias("flight_month"),
    F.col("flight_dt.week_of_year").alias("flight_week_of_year"),
    F.when(F.col("flight_dt.day_of_week").isin([6, 7]), 1).otherwise(0).alias("flight_is_weekend"),

    # 3. 实体ID特征 (后续需要进行数值化)
    F.col("departure_airport_fk"),
    F.col("arrival_airport_fk"),
    F.col("airline_fk"),
    F.col("flight_details_fk"),

    # 4. 其他数值特征
    F.col("total_travel_distance"),
    F.col("total_duration_in_minutes")
)

print("INFO: 基础特征已构建。")
features_df.show(5, truncate=False)
# 定义窗口：按同一个航班进行分区
# 一个航班由 出行日期、出发地、到达地、航司 联合定义
window_spec = Window.partitionBy(
    "flight_month", # 使用月份粗粒度分区，也可以用 flight_date_fk
    "departure_airport_fk",
    "arrival_airport_fk",
    "airline_fk"
).orderBy(F.col("total_fare").asc()) # 按价格升序排列

# 使用窗口函数计算
# 1. min_fare: 每个航班分区内的最低票价
# 2. rank: 每条记录在分区内的价格排名
label_df = features_df.withColumn(
    "min_fare_in_flight", F.min("total_fare").over(window_spec)
).withColumn(
    "fare_rank_in_flight", F.rank().over(window_spec)
)

# 定义“最佳购买窗口”标签 (is_best_window)
# 规则：如果当前票价在最低票价的115%以内，则认为是好时机 (1)
# 这个1.15是一个超参数，后续可以调整优化
final_df = label_df.withColumn(
    "is_best_window",
    F.when(
        F.col("total_fare") <= (F.col("min_fare_in_flight") * 1.15),
        1
    ).otherwise(0)
)

print("INFO: 'is_best_window' 分类标签已创建。")
# 查看一下结果，特别是高价和低价票的标签是否符合预期
final_df.select(
    "total_fare",
    "min_fare_in_flight",
    "fare_rank_in_flight",
    "is_best_window"
).show(10)
# 筛选最终需要的列作为ABT
abt_df = final_df.select(
    # --- 两个模型需要的标签 ---
    F.col("total_fare").alias("label_regression"),      # 回归模型标签
    F.col("is_best_window").alias("label_classification"), # 分类模型标签

    # --- 所有模型共用的特征 ---
    "search_to_flight_days",
    "flight_day_of_week",
    "flight_month",
    "flight_week_of_year",
    "flight_is_weekend",
    "departure_airport_fk",
    "arrival_airport_fk",
    "airline_fk",
    "flight_details_fk",
    "total_travel_distance",
    "total_duration_in_minutes"
).na.drop() # 删除任何包含空值的行，确保数据干净

print("INFO: 最终分析基础表 (ABT) 已构建完成。")
abt_df.printSchema()
abt_df.show(5)

# 将ABT保存为一张新的Hive表
try:
    abt_df.write.mode("overwrite").format("parquet").saveAsTable("flight_abt")
    print("SUCCESS: 分析基础表 'flight_abt' 已成功保存！")
except Exception as e:
    print(f"ERROR: 保存ABT失败: {e}")

# --- 结束 ---
spark.stop()