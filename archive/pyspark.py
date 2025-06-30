# -*- coding: utf-8 -*-

"""
最终版ETL脚本，用于构建航班数据仓库中的事实表.
作者: Gemini
更新日期: 2025-06-26
功能:
1. 从Hive表 `raw_itineraries` 加载原始数据.
2. 对源数据进行必要的类型转换（字符串转日期、数字等）.
3. 加载所有维度表 (`dim_date`, `dim_airline`, `dim_airport`, `dim_aircraft`).
4. 使用正确的业务键将事实数据与维度表进行左连接，以查找并填充外键.
5. 处理关联失败的情况，将NULL外键填充为0.
6. 构建最终的事实表 `fact_flight_ticket`.
7. 将结果以分区的Parquet格式写回Hive.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# ==============================================================================
# 1. 初始化 Spark Session
# ==============================================================================
spark = SparkSession.builder \
    .appName("FlightDW_FactTable_ETL_Final") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")

# ==============================================================================
# 2. 定义并使用数据库
# ==============================================================================
db_name = "flight_dw"
source_db = "flight_dw"
spark.sql(f"USE {db_name}")
print(f"INFO: 已切换到数据库: {db_name}")

# ==============================================================================
# 3. 加载所有维度表
# ==============================================================================
print("INFO: 开始从Hive加载所有维度表...")
try:
    dim_date_df = spark.table("dim_date").alias("dim_date")
    # 假设 dim_airline 的业务键是 airline_code
    dim_airline_df = spark.table("dim_airline").alias("dim_airline")
    # dim_airport_df = spark.table("dim_airport").alias("dim_airport")

    # --- 3. 创建并清洗【dim_airport】维度表 ---
    print("INFO: (1/4) 开始创建并清洗【dim_airport】维度表...")
    try:
        # 从源数据库加载机场原始数据
        raw_airports_df = spark.table(f"{source_db}.raw_airports")

        # 【核心修复】清洗并筛选数据，只保留 iata_code 有效的机场
        print("INFO: 正在过滤 iata_code 为空的无效机场数据...")
        clean_airports_df = raw_airports_df.filter(
            (F.col("iata_code").isNotNull()) & (F.trim(F.col("iata_code")) != "")
        ).filter(
            F.length(F.trim(F.col("iata_code"))) == 3  # 确保是3位IATA码
        )

        # 从清洗后的数据构建维度表
        dim_airport_df = clean_airports_df.select(
            F.col("ident"),  # 保留4位码作为主识别码
            F.col("iata_code"),  # 保留3位码用于与事实表关联
            F.col("name").alias("airport_name"),
            F.col("municipality").alias("city"),
            F.col("iso_country").alias("country_code")
        ).withColumn("airport_fk", F.monotonically_increasing_id())  # 添加代理键

        # 将干净的维度表数据写入Hive，覆盖旧的脏数据
        dim_airport_df.write.mode("overwrite").saveAsTable("dim_airport")
        print("SUCCESS: 【dim_airport】维度表已使用清洗过的数据重建。")

    except Exception as e:
        print(f"ERROR: 创建【dim_airport】时发生错误: {e}")
        spark.stop()
        exit(1)



    dim_aircraft_df = spark.table("dim_aircraft").alias("dim_aircraft")
    print("SUCCESS: 所有维度表加载成功！")
    print("--- 维度表示例 ---")
    dim_airport_df.select("airport_fk", "airport_code", "airport_name").show(3, truncate=False)
    dim_aircraft_df.select("aircraft_fk", "equipment_description").show(3, truncate=False)
except Exception as e:
    print(f"ERROR: 加载维度表时发生错误: {e}")
    spark.stop()
    exit(1)

# ==============================================================================
# 4. 加载并转换事实表源数据 (raw_itineraries)
# ==============================================================================
print("INFO: 开始从Hive表 `raw_itineraries` 加载事实数据...")
raw_itineraries_df = spark.table("raw_itineraries")

print("INFO: 数据加载完成，开始进行关键的类型转换...")
# 这一步至关重要，将所有string列转换为正确的业务类型
fact_base_df = raw_itineraries_df.withColumn(
    # 日期转换
    "search_date_dt", F.to_date(F.col("search_date"), "yyyy-MM-dd")
).withColumn(
    "flight_date_dt", F.to_date(F.col("flight_date"), "yyyy-MM-dd")
).withColumn(
    # 数字转换
    "base_fare_num", F.col("base_fare").cast(FloatType())
).withColumn(
    "total_fare_num", F.col("total_fare").cast(FloatType())
).withColumn(
    "seats_remaining_num", F.col("seats_remaining").cast(IntegerType())
).withColumn(
    "segments_distance_num", F.col("segments_distance").cast(IntegerType())
).withColumn(
    # 时间转换：秒 -> 分钟
    "total_duration_in_minutes", (F.col("segments_duration_in_seconds") / 60).cast(IntegerType())
)

print("SUCCESS: 类型转换完成！")
fact_base_df.printSchema()

# ==============================================================================
# 5. 关联事实与维度，生成外键 (最核心的步骤)
# ==============================================================================
print("INFO: 开始关联事实表与维度表...")

# 为每个DataFrame设置别名，这是最佳实践，可以避免任何列名冲突
fact_aliased = fact_base_df.alias("fact")
dim_date_aliased_search = dim_date_df.alias("search_dt")
dim_date_aliased_flight = dim_date_df.alias("flight_dt")
dim_airline_aliased = dim_airline_df.alias("al")
dim_airport_aliased_dep = dim_airport_df.alias("dep_ap")
dim_airport_aliased_arr = dim_airport_df.alias("arr_ap")
dim_aircraft_aliased = dim_aircraft_df.alias("ac")

final_joined_df = fact_aliased \
    .join(dim_date_aliased_search, F.col("fact.search_date_dt") == F.col("search_dt.full_date"), "left_outer") \
    .join(dim_date_aliased_flight, F.col("fact.flight_date_dt") == F.col("flight_dt.full_date"), "left_outer") \
    .join(
        dim_airline_aliased,
        F.trim(F.upper(F.col("fact.segments_airline_code"))) == F.trim(F.upper(F.col("al.airline_code"))),
        "left_outer"
    ) \
    .join(
        dim_airport_aliased_dep,
        # **最终修正**: 使用 dim_airport.airport_code 进行关联
        F.trim(F.upper(F.col("fact.segments_departure_airport"))) == F.trim(F.upper(F.col("dep_ap.airport_code"))),
        "left_outer"
    ) \
    .join(
        dim_airport_aliased_arr,
        # **最终修正**: 使用 dim_airport.airport_code 进行关联
        F.trim(F.upper(F.col("fact.segments_arrival_airport"))) == F.trim(F.upper(F.col("arr_ap.airport_code"))),
        "left_outer"
    ) \
    .join(
        dim_aircraft_aliased,
        # **最终修正**: 使用 dim_aircraft.equipment_description 进行关联
        F.trim(F.upper(F.col("fact.segments_equipment_description"))) == F.trim(F.upper(F.col("ac.equipment_description"))),
        "left_outer"
    )

print("SUCCESS: 事实表与维度表关联完成！")

# ==============================================================================
# 6. 构建最终事实表 DataFrame
# ==============================================================================
print("INFO: 开始构建最终事实表DataFrame，并选择最终列...")
fact_flight_ticket_df = final_joined_df.select(
    # --- 外键 (Surrogate Keys) ---
    F.col("search_dt.date_fk").alias("search_date_fk"),
    F.col("flight_dt.date_fk").alias("flight_date_fk"),
    F.col("al.airline_fk").alias("airline_fk"),
    F.col("dep_ap.airport_fk").alias("departure_airport_fk"),
    F.col("arr_ap.airport_fk").alias("arrival_airport_fk"),
    F.col("ac.aircraft_fk").alias("aircraft_fk"),
    F.lit(0).alias("flight_details_fk"),  # 业务逻辑待定的外键, 暂时设为0

    # --- 度量值 (Measures) 和 事实属性 (Degenerate Dimensions) ---
    F.col("fact.leg_id"),
    F.col("fact.base_fare_num").alias("base_fare"),
    F.col("fact.total_fare_num").alias("total_fare"),
    F.col("fact.seats_remaining_num").alias("seats_remaining"),
    F.col("fact.segments_distance_num").alias("total_travel_distance"),
    F.col("fact.total_duration_in_minutes"),
    F.datediff(F.col("fact.flight_date_dt"), F.col("fact.search_date_dt")).alias("search_to_flight_days"),

    # --- 分区字段 (必须从转换后的日期列生成) ---
    F.year(F.col("fact.flight_date_dt")).alias("year"),
    F.month(F.col("fact.flight_date_dt")).alias("month"),
    F.dayofmonth(F.col("fact.flight_date_dt")).alias("day")
)

# 关键一步：处理关联失败的情况，将所有NULL外键填充为0
fk_columns = [
    "search_date_fk", "flight_date_fk", "airline_fk",
    "departure_airport_fk", "arrival_airport_fk", "aircraft_fk", "flight_details_fk"
]
fact_flight_ticket_final_df = fact_flight_ticket_df.na.fill(0, subset=fk_columns)

print("SUCCESS: 最终事实表DataFrame构建完成！")
print("--- 最终数据结构 Schema ---")
fact_flight_ticket_final_df.printSchema()
print("--- 最终数据预览 (前20行) ---")
fact_flight_ticket_final_df.show(20, truncate=False)

# ==============================================================================
# 7. 将结果写入 Hive 事实表
# ==============================================================================
TARGET_TABLE = "fact_flight_ticket"
print(f"INFO: 开始将数据写入Hive事实表: `{TARGET_TABLE}`...")
try:
    fact_flight_ticket_final_df.write \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .format("parquet") \
        .saveAsTable(TARGET_TABLE)
    print(f"SUCCESS: ETL过程成功完成！数据已覆写到 `{TARGET_TABLE}` 表。")
except Exception as e:
    print(f"FATAL: 写入Hive表时发生严重错误: {e}")
    spark.stop()
    exit(1)

# ==============================================================================
# 8. 关闭 Spark Session
# ==============================================================================
print("INFO: 所有任务完成，关闭Spark Session。")
spark.stop()