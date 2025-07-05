 # 6.27 15.34
# -*- coding: utf-8 -*-


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, BooleanType

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
    spark.catalog.refreshTable("flight_dw.raw_airports")
    # 从源数据库加载机场原始数据
    raw_airports_df = spark.table(f"{source_db}.raw_airports")

    # 【核心修复】清洗并筛选数据，只保留 iata_code 有效的机场
    print("INFO: 正在过滤 iata_code 为空的无效机场数据...")
    clean_airports_df = raw_airports_df.filter(
        (F.col("iata_code").isNotNull()) & (F.trim(F.col("iata_code")) != "")
    )

    # 从清洗后的数据构建维度表
    dim_airport_df = clean_airports_df.select(
        F.col("ident"),  # 保留4位码作为主识别码



        # F.col("iata_code"),  # 保留3位码用于与事实表关联
        F.regexp_replace(F.col("iata_code"), '"', '').alias("iata_code"),



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
    raw_itineraries_df = spark.table(f"{source_db}.raw_itineraries")

    # 提取所有不重复的飞机设备描述
    dim_aircraft_df = raw_itineraries_df.select(
        F.col("segments_equipment_description").alias("equipment_description")
    ).filter(
        F.col("equipment_description").isNotNull() & (F.trim(F.col("equipment_description")) != "")
    ).distinct().withColumn("aircraft_fk", F.monotonically_increasing_id())

    dim_aircraft_df.write.mode("overwrite").saveAsTable("dim_aircraft")
    print("SUCCESS: 【dim_aircraft】维度表已成功创建。")

except Exception as e:
    print(f"WARN: 创建【dim_aircraft】时发生错误，将继续执行: {e}")

# dim_airport_detail
# --- 5. 【新增】创建并关联【dim_flight_details】维度表 ---
print("INFO: (3/5) 开始创建并关联【dim_flight_details】维度表...")
try:
    details_cols = ["fare_basis_code", "segments_cabin_code", "is_basic_economy", "is_refundable"]
    # 再次使用 raw_itineraries_df
    # 提取所有不重复的“机票产品属性”组合
    raw_itineraries_df = spark.table(f"{source_db}.raw_itineraries")
    dim_flight_details_df = raw_itineraries_df.select(
        F.col("fare_basis_code"),
        F.col("segments_cabin_code"),
        F.col("is_basic_economy").cast(BooleanType()),
        F.col("is_refundable").cast(BooleanType())
    ).na.drop(subset=details_cols).distinct().withColumn("flight_details_fk", F.monotonically_increasing_id())

    dim_flight_details_df.write.mode("overwrite").saveAsTable("dim_flight_details")
    print("SUCCESS: 【dim_flight_details】维度表已成功创建。")
except Exception as e:
    print(f"ERROR: 创建【dim_flight_details】时发生错误: {e}")
    # ...





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

# --- 5. 加载所有已创建的、干净的维度表 ---
print("INFO: 开始加载所有已清洗的维度表...")
try:
    dim_date_df = spark.table("dim_date")
    dim_airline_df = spark.table("dim_airline")
    dim_airport_df = spark.table("dim_airport")
    dim_aircraft_df = spark.table("dim_aircraft")
    dim_flight_details_df = spark.table("dim_flight_details")  # 加载新建的维度
    print("SUCCESS: 所有维度表加载成功！")
except Exception as e:
    print(f"ERROR: 加载维度表失败: {e}")
    spark.stop()
    exit(1)

# --- 6. 加载事实表源数据并进行核心的拆分转换 ---
print("INFO: 开始加载事实表源数据 `raw_itineraries`...")
raw_itineraries_df = spark.table("flight_dw.raw_itineraries") # 假设源数据在 default 库

print("INFO: 【核心步骤】开始拆分 '||' 分隔的多航段数据...")
# 使用 split 函数将相关字段拆分为数组。注意 '||' 需要转义成 '\|\|'
split_df = raw_itineraries_df.withColumn(
    "segments_departure_airport_arr", F.split(F.col("segments_departure_airport"), "\|\|")
).withColumn(
    "segments_arrival_airport_arr", F.split(F.col("segments_arrival_airport"), "\|\|")
).withColumn(
    "segments_airline_code_arr", F.split(F.col("segments_airline_code"), "\|\|")
).withColumn(
    "segments_distance_arr", F.split(F.col("segments_distance"), "\|\|")
).withColumn(
    "segments_duration_in_seconds_arr", F.split(F.col("segments_duration_in_seconds"), "\|\|")
).withColumn(
    "segments_equipment_description_arr", F.split(F.col("segments_equipment_description"), "\|\|")
)

# 使用 arrays_zip 将拆分后的多个数组“拉链式”地合并成一个结构体数组
# 这样可以确保每个航段的出发、到达、航司等信息都配对在一起
zipped_df = split_df.withColumn(
    "legs",
    F.arrays_zip(
        "segments_departure_airport_arr", "segments_arrival_airport_arr",
        "segments_airline_code_arr", "segments_distance_arr",
        "segments_duration_in_seconds_arr", "segments_equipment_description_arr"
    )
)

# 使用 explode 函数，将每个行程(row)的多个航段(legs)“爆炸”成多行
# 这样，我们就得到了以“航段”为粒度的基础事实表
fact_base_df = zipped_df.withColumn("leg_data", F.explode(F.col("legs")))
print("SUCCESS: 多航段数据已成功拆分为单航段记录。")

# --- 7. 对拆分后的数据进行类型转换 ---
print("INFO: 开始对拆分后的航段数据进行类型转换...")
# 从 `leg_data` 结构体中提取每个航段的具体信息
fact_base_transformed_df = fact_base_df.select(
    # 行程级别的列，在爆炸后会被复制到每个航段行
    "leg_id", "search_date", "flight_date", "base_fare", "total_fare", "seats_remaining",
    "fare_basis_code", "segments_cabin_code", "is_basic_economy", "is_refundable",
    # 从 leg_data 结构体中提取航段级别的列
    F.col("leg_data.segments_departure_airport_arr").alias("departure_airport"),
    F.col("leg_data.segments_arrival_airport_arr").alias("arrival_airport"),
    F.col("leg_data.segments_airline_code_arr").alias("airline_code"),
    F.col("leg_data.segments_equipment_description_arr").alias("equipment_description"),
    # 类型转换
    F.col("leg_data.segments_distance_arr").cast(IntegerType()).alias("leg_distance"),
    (F.col("leg_data.segments_duration_in_seconds_arr") / 60).cast(IntegerType()).alias("leg_duration_in_minutes")
).withColumn(
    "search_date_dt", F.to_date(F.col("search_date"), "yyyy-MM-dd")
).withColumn(
    "flight_date_dt", F.to_date(F.col("flight_date"), "yyyy-MM-dd")
).withColumn(
    "base_fare_num", F.col("base_fare").cast(FloatType())
).withColumn(
    "total_fare_num", F.col("total_fare").cast(FloatType())
).withColumn(
    "seats_remaining_num", F.col("seats_remaining").cast(IntegerType())
)
print("SUCCESS: 类型转换完成。")


# --- 8. 关联事实与维度 ---
print("INFO: 开始关联事实表与维度表...")
fact_aliased = fact_base_transformed_df.alias("fact")
dim_date_aliased_search = dim_date_df.alias("search_dt")
dim_date_aliased_flight = dim_date_df.alias("flight_dt")
dim_airline_aliased = dim_airline_df.alias("al")
dim_airport_aliased_dep = dim_airport_df.alias("dep_ap")
dim_airport_aliased_arr = dim_airport_df.alias("arr_ap")
dim_aircraft_aliased = dim_aircraft_df.alias("ac")

final_joined_df = fact_aliased \
    .join(dim_date_aliased_search, F.col("fact.search_date_dt") == F.col("search_dt.full_date"), "left_outer") \
    .join(dim_date_aliased_flight, F.col("fact.flight_date_dt") == F.col("flight_dt.full_date"), "left_outer") \
    .join(dim_airline_aliased, F.trim(F.upper(F.col("fact.airline_code"))) == F.trim(F.upper(F.col("al.airline_code"))), "left_outer") \
    .join(dim_airport_aliased_dep, F.trim(F.upper(F.col("fact.departure_airport"))) == F.trim(F.upper(F.col("dep_ap.iata_code"))), "left_outer") \
    .join(dim_airport_aliased_arr, F.trim(F.upper(F.col("fact.arrival_airport"))) == F.trim(F.upper(F.col("arr_ap.iata_code"))), "left_outer") \
    .join(dim_aircraft_aliased,
          F.trim(F.upper(F.col("fact.equipment_description"))) == F.trim(F.upper(F.col("ac.equipment_description"))),
          "left_outer") \
    .join(
    dim_flight_details_df.alias("fd"),
        # 【关键修复】确保关联时，事实表和维度表的字段类型和值都经过了一致的处理
        (F.col("fact.fare_basis_code") == F.col("fd.fare_basis_code")) &
        (F.col("fact.segments_cabin_code") == F.col("fd.segments_cabin_code")) &
        (F.col("fact.is_basic_economy").cast(BooleanType()) == F.col("fd.is_basic_economy")) &
        (F.col("fact.is_refundable").cast(BooleanType()) == F.col("fd.is_refundable")),
        "left_outer"
    )
print("SUCCESS: 关联操作完成。")


# --- 9. 构建并写入最终事实表 ---
print("INFO: 构建并写入最终的 `fact_flight_ticket` 表...")
fact_flight_ticket_df = final_joined_df.select(
    F.col("search_dt.date_fk").alias("search_date_fk"),
    F.col("flight_dt.date_fk").alias("flight_date_fk"),
    F.col("al.airline_fk").alias("airline_fk"),
    F.col("dep_ap.airport_fk").alias("departure_airport_fk"),
    F.col("arr_ap.airport_fk").alias("arrival_airport_fk"),
    F.col("ac.aircraft_fk").alias("aircraft_fk"),

    F.col("fd.flight_details_fk").alias("flight_details_fk"),
    F.col("fact.leg_id"),
    F.col("fact.base_fare_num").alias("base_fare"),
    F.col("fact.total_fare_num").alias("total_fare"),
    F.col("fact.seats_remaining_num").alias("seats_remaining"),
    F.col("fact.leg_distance").alias("total_travel_distance"), # 使用拆分后的航段距离
    F.col("fact.leg_duration_in_minutes").alias("total_duration_in_minutes"), # 使用拆分后的航段时长
    F.datediff(F.col("fact.flight_date_dt"), F.col("fact.search_date_dt")).alias("search_to_flight_days"),
    F.year(F.col("fact.flight_date_dt")).alias("year"),
    F.month(F.col("fact.flight_date_dt")).alias("month"),
    F.dayofmonth(F.col("fact.flight_date_dt")).alias("day")
)

fk_columns = ["search_date_fk", "flight_date_fk", "airline_fk", "departure_airport_fk", "arrival_airport_fk", "aircraft_fk", "flight_details_fk"]
fact_flight_ticket_final_df = fact_flight_ticket_df.na.fill(0, subset=fk_columns)

fact_flight_ticket_final_df.write.partitionBy("year", "month", "day").mode("overwrite").format("parquet").saveAsTable("fact_flight_ticket")
print("SUCCESS: ETL全流程成功完成！`fact_flight_ticket` 表已更新。")

# --- 10. 关闭 Spark Session ---
spark.stop()

