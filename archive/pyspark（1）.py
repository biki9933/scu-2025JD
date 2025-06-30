# -*- coding: utf-8 -*-

"""
最终重构版ETL脚本，用于构建航班数据仓库.
作者: Gemini & 贡献者(用户)
更新日期: 2025-06-26
功能:
1.  在一个脚本内，按正确顺序执行所有ETL操作。
2.  【关键修复】在创建维度表（尤其是dim_airport）时，增加严格的清洗和过滤逻辑。
3.  使用清洗干净的维度表来构建最终的事实表 fact_flight_ticket。
4.  将所有结果以分区的Parquet格式写回Hive。
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# ==============================================================================
# 1. 初始化 Spark Session
# ==============================================================================
spark = SparkSession.builder \
    .appName("FlightDW_ETL_Complete_Final") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")

# ==============================================================================
# 2. 定义并使用数据库
# ==============================================================================
# 假设你的原始数据在 'default' 库, 目标数据仓库在 'flight_dw' 库
# 如果库名不同, 请修改这里的变量
source_db = "flight_dw"
target_db = "flight_dw"

spark.sql(f"USE {target_db}")
print(f"INFO: 已切换到目标数据库: {target_db}")

# ==============================================================================
# PART I: 创建并清洗【维度表】
# ==============================================================================

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

# --- 4. 创建其他维度表 (以 dim_aircraft, dim_airline, dim_date 为例) ---
# 你可以按照类似上面的模式，在这里创建其他所有维度表
print("INFO: (2/4) 开始创建【dim_aircraft】维度表...")
try:
    # 请确保你的原始飞机数据表名是正确的
    raw_aircrafts_df = spark.table(f"{source_db}.raw_aircraft")

    dim_aircraft_df = raw_aircrafts_df.select(
        F.col("description").alias("equipment_description"),
        F.col("manufacturer")
    ).distinct().withColumn("aircraft_fk", F.monotonically_increasing_id())

    dim_aircraft_df.write.mode("overwrite").saveAsTable("dim_aircraft")
    print("SUCCESS: 【dim_aircraft】维度表已创建。")

except Exception as e:
    print(f"WARN: 创建【dim_aircraft】时发生错误，将继续执行: {e}")

print("INFO: (3/4) 开始创建【dim_airline】维度表...")
try:
    # 假设从 `raw_itineraries` 提取航司信息来创建维度表
    raw_itineraries_for_airline_df = spark.table(f"{source_db}.raw_itineraries")

    dim_airline_df = raw_itineraries_for_airline_df.select(
        F.col("segments_airline_code").alias("airline_code"),
        F.col("segments_airline_name").alias("airline_name")
    ).filter(
        F.col("airline_code").isNotNull() & (F.trim(F.col("airline_code")) != "")
    ).distinct().withColumn("airline_fk", F.monotonically_increasing_id())

    dim_airline_df.write.mode("overwrite").saveAsTable("dim_airline")
    print("SUCCESS: 【dim_airline】维度表已创建。")
except Exception as e:
    print(f"WARN: 创建【dim_airline】时发生错误，将继续执行: {e}")

print("INFO: (4/4) 开始创建【dim_date】维度表...")
try:
    # 从事实表中提取所有不重复的日期，用于构建日期维度
    raw_itineraries_for_date_df = spark.table(f"{source_db}.raw_itineraries")
    date_df1 = raw_itineraries_for_date_df.select(F.to_date("search_date").alias("full_date"))
    date_df2 = raw_itineraries_for_date_df.select(F.to_date("flight_date").alias("full_date"))

    distinct_dates_df = date_df1.union(date_df2).distinct().filter(F.col("full_date").isNotNull())

    dim_date_df = distinct_dates_df.select(
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.month("full_date").alias("month"),
        F.dayofmonth("full_date").alias("day"),
        F.quarter("full_date").alias("quarter"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.dayofyear("full_date").alias("day_of_year"),
        F.weekofyear("full_date").alias("week_of_year")
    ).withColumn("date_fk", F.monotonically_increasing_id())

    dim_date_df.write.mode("overwrite").saveAsTable("dim_date")
    print("SUCCESS: 【dim_date】维度表已创建。")
except Exception as e:
    print(f"ERROR: 创建【dim_date】时发生错误: {e}")
    spark.stop()
    exit(1)

# ==============================================================================
# PART II: 创建【事实表】
# ==============================================================================

# --- 5. 加载所有刚刚创建的、干净的维度表 ---
print("INFO: 开始加载所有已清洗的维度表，用于构建事实表...")
try:
    dim_date_df = spark.table("dim_date")
    dim_airline_df = spark.table("dim_airline")
    dim_airport_df = spark.table("dim_airport")  # 加载修复后的 dim_airport
    dim_aircraft_df = spark.table("dim_aircraft")
    print("SUCCESS: 所有维度表加载成功！")
except Exception as e:
    print(f"ERROR: 加载维度表失败，无法构建事实表: {e}")
    spark.stop()
    exit(1)

# --- 6. 加载并转换事实表源数据 (raw_itineraries) ---
print("INFO: 开始加载并转换事实表源数据 `raw_itineraries`...")
raw_itineraries_df = spark.table(f"{source_db}.raw_itineraries")
fact_base_df = raw_itineraries_df.withColumn(
    "search_date_dt", F.to_date(F.col("search_date"), "yyyy-MM-dd")
).withColumn(
    "flight_date_dt", F.to_date(F.col("flight_date"), "yyyy-MM-dd")
).withColumn(
    "base_fare_num", F.col("base_fare").cast(FloatType())
).withColumn(
    "total_fare_num", F.col("total_fare").cast(FloatType())
).withColumn(
    "seats_remaining_num", F.col("seats_remaining").cast(IntegerType())
).withColumn(
    "segments_distance_num", F.col("segments_distance").cast(IntegerType())
).withColumn(
    "total_duration_in_minutes", (F.col("segments_duration_in_seconds") / 60).cast(IntegerType())
)
print("SUCCESS: 事实表源数据转换完成。")

# --- 7. 关联事实与维度 ---
print("INFO: 开始关联事实表与已清洗的维度表...")
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
    .join(dim_airline_aliased,
          F.trim(F.upper(F.col("fact.segments_airline_code"))) == F.trim(F.upper(F.col("al.airline_code"))),
          "left_outer") \
    .join(
    dim_airport_aliased_dep,
    # 【最终关联逻辑】使用事实表中的机场代码与维度表中干净的 iata_code 进行关联
    F.trim(F.upper(F.coalesce(F.col("fact.segments_departure_airport"), F.col("fact.starting_airport")))) == F.trim(
        F.upper(F.col("dep_ap.iata_code"))),
    "left_outer"
) \
    .join(
    dim_airport_aliased_arr,
    F.trim(F.upper(F.coalesce(F.col("fact.segments_arrival_airport"), F.col("fact.destination_airport")))) == F.trim(
        F.upper(F.col("arr_ap.iata_code"))),
    "left_outer"
) \
    .join(dim_aircraft_aliased, F.trim(F.upper(F.col("fact.segments_equipment_description"))) == F.trim(
    F.upper(F.col("ac.equipment_description"))), "left_outer")
print("SUCCESS: 关联操作完成。")

# --- 8. 构建并写入最终事实表 ---
print("INFO: 构建并写入最终的 `fact_flight_ticket` 表...")
fact_flight_ticket_df = final_joined_df.select(
    F.col("search_dt.date_fk").alias("search_date_fk"),
    F.col("flight_dt.date_fk").alias("flight_date_fk"),
    F.col("al.airline_fk").alias("airline_fk"),
    F.col("dep_ap.airport_fk").alias("departure_airport_fk"),
    F.col("arr_ap.airport_fk").alias("arrival_airport_fk"),
    F.col("ac.aircraft_fk").alias("aircraft_fk"),
    F.lit(0).alias("flight_details_fk"),
    F.col("fact.leg_id"),
    F.col("fact.base_fare_num").alias("base_fare"),
    F.col("fact.total_fare_num").alias("total_fare"),
    F.col("fact.seats_remaining_num").alias("seats_remaining"),
    F.col("fact.segments_distance_num").alias("total_travel_distance"),
    F.col("fact.total_duration_in_minutes"),
    F.datediff(F.col("fact.flight_date_dt"), F.col("fact.search_date_dt")).alias("search_to_flight_days"),
    F.year(F.col("fact.flight_date_dt")).alias("year"),
    F.month(F.col("fact.flight_date_dt")).alias("month"),
    F.dayofmonth(F.col("fact.flight_date_dt")).alias("day")
)

fk_columns = ["search_date_fk", "flight_date_fk", "airline_fk", "departure_airport_fk", "arrival_airport_fk",
              "aircraft_fk", "flight_details_fk"]
fact_flight_ticket_final_df = fact_flight_ticket_df.na.fill(0, subset=fk_columns)

fact_flight_ticket_final_df.write.partitionBy("year", "month", "day").mode("overwrite").format("parquet").saveAsTable(
    "fact_flight_ticket")
print("SUCCESS: ETL全流程成功完成！`fact_flight_ticket` 表已更新。")

# --- 9. 关闭 Spark Session ---
spark.stop()