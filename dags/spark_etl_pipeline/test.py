# -*- coding: utf-8 -*-
#
# ==============================================================================
# 项目: 机票元搜索及智能推荐 - 数据仓库ETL脚本
#
# 描述:
#   本脚本负责执行完整的数据ETL流程，将HDFS上的原始数据进行抽取(Extract)、
#   转换(Transform)和加载(Load)，最终生成符合星型模型的事实表和维度表。
#
# 核心逻辑:
#   1. 从Hive原始层(raw_*)加载数据。
#   2. 对多航段行程数据进行“爆炸”处理，将一条行程记录拆分为多条航段记录。
#   3. 清洗、转换数据，并生成所有维度表 (dim_*)，包括动态生成代理键(surrogate keys)。
#   4. 将处理后的航段数据与所有维度表关联，生成最终的机票行程事实表(fact_flight_ticket)。
#   5. 使用动态分区技术将事实表数据写入Hive。
#
# 作者: Gemini AI
# 版本: 1.0 (适配您的项目)
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def main():
    """
    主ETL处理函数
    """
    # ==========================================================================
    # 0. 初始化与配置
    # ==========================================================================

    # 【请修改】设置您的Hive数据库名称
    db_name = "flight_dw"

    # 初始化支持Hive的SparkSession
    spark = SparkSession.builder \
        .appName("FlightDW_ETL_For_Your_Project") \
        .config("spark.sql.exec.dynamicPartition", "true") \
        .config("spark.sql.exec.dynamicPartition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Spark Session 已创建，开始执行ETL流程...")
    print(f"目标数据库: {db_name}")

    # ==========================================================================
    # 1. 抽取 (Extract): 加载原始数据表
    # ==========================================================================
    print("阶段 1: 正在从Hive加载原始数据...")

    # 从Hive加载行程数据和机场数据
    # 注意：这里的表是您上一步创建的raw_表
    itineraries_raw_df = spark.table(f"{db_name}.raw_itineraries")
    airports_raw_df = spark.table(f"{db_name}.raw_airports")

    print("原始数据加载完成。")

    # ==========================================================================
    # 2. 转换 (Transform): 航段“爆炸”与预处理 (核心逻辑)
    # ==========================================================================
    print("阶段 2: 正在处理多航段行程数据 (爆炸操作)...")

    # 定义需要按分隔符 '||' 拆分的列
    # 注意: 这里我们假设您的CSV中复杂字段是用 '||' 分隔的，这比用逗号更安全
    split_cols = [
        'segments_departure_time_epoch_seconds', 'segments_departure_time_raw',
        'segments_arrival_time_epoch_seconds', 'segments_arrival_time_raw',
        'segments_arrival_airport', 'segments_departure_airport',
        'segments_airline_name', 'segments_airline_code',
        'segments_equipment_description', 'segments_duration_in_seconds',
        'segments_distance', 'segments_cabin_code'
    ]

    segments_df = itineraries_raw_df
    for col_name in split_cols:
        # 将 "col_name" 列按 '||' 分割成数组，并存入 "col_name_arr" 新列
        segments_df = segments_df.withColumn(f"{col_name}_arr", F.split(F.col(col_name), "\\|\\|"))

    # 使用 posexplode 将数组炸开，为每个航段生成一条记录
    # posexplode会生成两列：pos(数组中的位置)和col(数组中的值)
    exploded_df = segments_df.select(
        "*", F.posexplode("segments_departure_airport_arr").alias("pos", "segment_departure_code")
    )

    # 整理和清洗爆炸后的数据，形成干净的航段数据表
    final_segments_df = exploded_df.select(
        F.col("leg_id"),
        F.to_date(F.col("search_date"), "yyyy-MM-dd").alias("search_date_dt"),
        F.to_date(F.col("flight_date"), "yyyy-MM-dd").alias("flight_date_dt"),
        F.col("is_refundable").cast("boolean"),
        F.col("is_basic_economy").cast("boolean"),
        F.col("is_non_stop").cast("boolean"),
        F.col("base_fare").cast("double"),
        F.col("total_fare").cast("double"),
        F.col("seats_remaining").cast("int"),
        F.col("total_travel_distance").cast("int"),
        (F.col("pos") + 1).alias("segment_order"),
        F.col("segment_departure_code"),
        F.col("segments_arrival_airport_arr")[F.col("pos")].alias("segment_arrival_code"),
        F.col("segments_airline_name_arr")[F.col("pos")].alias("airline_name"),
        F.col("segments_airline_code_arr")[F.col("pos")].alias("airline_code"),
        F.trim(F.col("segments_equipment_description_arr")[F.col("pos")]).alias("equipment_description"),
        F.col("segments_duration_in_seconds_arr")[F.col("pos")].cast("int").alias("segment_duration_in_seconds")
    ).filter(F.col("flight_date_dt").isNotNull()) # 过滤掉航班日期为空的无效数据

    # 将处理后的航段数据缓存，因为后续所有维度表和事实表都将基于它来创建
    final_segments_df.cache()
    print("多航段数据处理完成。")
    print(f"总航段数: {final_segments_df.count()}")

    # ==========================================================================
    # 3. 转换与加载 (Transform & Load): 创建并加载所有维度表
    # ==========================================================================
    print("阶段 3: 正在创建并加载所有维度表...")

    # --- 3.1 dim_date ---
    # 此部分逻辑是正确的，无需修改
    print("  正在处理 dim_date...")
    date_df = final_segments_df.select(F.col("search_date_dt").alias("full_date")).union(
        final_segments_df.select(F.col("flight_date_dt").alias("full_date"))
    ).distinct().filter(F.col("full_date").isNotNull())

    dim_date_df = date_df.select(
        F.monotonically_increasing_id().alias("date_fk").cast("INT"),
        F.col("full_date"),
        F.year(F.col("full_date")).alias("year_of_date"),
        F.month(F.col("full_date")).alias("month_of_date"),
        F.dayofmonth(F.col("full_date")).alias("day_of_month"),
        F.dayofweek(F.col("full_date")).alias("day_of_week").cast("STRING"),
        F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
    )
    dim_date_df.write.mode("overwrite").insertInto(f"{db_name}.dim_date")
    dim_date_df.cache()
    print(f"  dim_date 加载完成. 共 {dim_date_df.count()} 条记录.")

    # --- 3.2 dim_airport ---
    print("  正在处理 dim_airport...")
    dim_airport_df = airports_raw_df.select(
        F.col("iata_code").alias("airport_code"),
        F.col("name").alias("airport_name"),
        F.col("municipality").alias("city"),
        F.col("iso_country").alias("country_code")
    ).filter(F.col("airport_code").isNotNull()).distinct().withColumn(
        "airport_fk", F.monotonically_increasing_id().cast("INT")
    ).select("airport_fk", "airport_code", "airport_name", "city", "country_code")

    dim_airport_df.write.mode("overwrite").insertInto(f"{db_name}.dim_airport")
    dim_airport_df.cache()
    print(f"  dim_airport 加载完成. 共 {dim_airport_df.count()} 条记录.")

    # --- 3.3 dim_airline ---
    print("  正在处理 dim_airline...")
    dim_airline_df = final_segments_df.select("airline_code", "airline_name").filter(
        F.col("airline_code").isNotNull()).distinct() \
        .withColumn("alliance_name", F.lit(None).cast(StringType())) \
        .withColumn("airline_fk", F.monotonically_increasing_id().cast("INT"))
    .select("airline_fk", "airline_code", "airline_name", "alliance_name")


    dim_airline_df.write.mode("overwrite").insertInto(f"{db_name}.dim_airline")
    dim_airline_df.cache()
print(f"  dim_airline 加载完成. 共 {dim_airline_df.count()} 条记录.")

# --- 3.4 dim_aircraft (动态创建) ---
print("  正在处理 dim_aircraft...")
dim_aircraft_df = final_segments_df.select(F.trim(F.col("equipment_description")).alias("equipment_description")) \
    .filter(F.col("equipment_description") != "") \
    .distinct() \
    .withColumn("manufacturer", F.lit(None).cast(StringType())) \
    .withColumn("aircraft_fk", F.monotonically_increasing_id().cast("INT"))  # 【修正】添加 .cast("INT") \
.select("aircraft_fk", "equipment_description", "manufacturer")  # 【最佳实践】调整列顺序

dim_aircraft_df.write.mode("overwrite").insertInto(f"{db_name}.dim_aircraft")
dim_aircraft_df.cache()
print(f"  dim_aircraft 加载完成. 共 {dim_aircraft_df.count()} 条记录.")

# --- 3.5 dim_flight_details (垃圾维度) ---
print("  正在处理 dim_flight_details...")
dim_flight_details_df = final_segments_df.select("is_refundable", "is_basic_economy", "is_non_stop").distinct() \
    .withColumn("flight_number", F.lit(None).cast(StringType())) \
    .withColumn("flight_details_fk", F.monotonically_increasing_id().cast("INT"))  # 【修正】添加 .cast("INT") \
.select("flight_details_fk", "flight_number", "is_refundable", "is_basic_economy", "is_non_stop")  # 【最佳实践】调整列顺序

dim_flight_details_df.write.mode("overwrite").insertInto(f"{db_name}.dim_flight_details")
dim_flight_details_df.cache()
print(f"  dim_flight_details 加载完成. 共 {dim_flight_details_df.count()} 条记录.")

# ==========================================================================
# 4. 加载 (Load): 创建并加载事实表 (最终关联)
# ==========================================================================
# 此部分逻辑是正确的，无需修改
print("阶段 4: 正在关联维度表并生成最终的事实表...")

# 将航段表与所有维度表进行左连接 (left join)，以获取代理键
fact_joined_df = final_segments_df \
    .join(dim_date_df.alias("search_dt"), F.col("search_date_dt") == F.col("search_dt.full_date"), "left") \
    .join(dim_date_df.alias("flight_dt"), F.col("flight_date_dt") == F.col("flight_dt.full_date"), "left") \
    .join(dim_airline_df, ["airline_code"], "left") \
    .join(dim_airport_df.alias("dep_ap"), F.col("segment_departure_code") == F.col("dep_ap.airport_code"), "left") \
    .join(dim_airport_df.alias("arr_ap"), F.col("segment_arrival_code") == F.col("arr_ap.airport_code"), "left") \
    .join(dim_aircraft_df, ["equipment_description"], "left") \
    .join(dim_flight_details_df, ["is_refundable", "is_basic_economy", "is_non_stop"], "left")

# 按照 xin.sql 中定义的 fact_flight_ticket 表结构，选择并重命名列
final_fact_df = fact_joined_df.select(
    # Foreign Keys
    F.col("search_dt.date_fk").alias("search_date_fk"),
    F.col("flight_dt.date_fk").alias("flight_date_fk"),
    F.col("airline_fk"),
    F.col("dep_ap.airport_fk").alias("departure_airport_fk"),
    F.col("arr_ap.airport_fk").alias("arrival_airport_fk"),
    F.col("aircraft_fk"),
    F.col("flight_details_fk"),
    # Degenerate Dimension
    F.col("leg_id"),
    # Measures
    F.col("base_fare"),
    F.col("total_fare"),
    F.col("seats_remaining"),
    F.col("total_travel_distance"),
    (F.col("segment_duration_in_seconds") / 60).cast("int").alias("total_duration_in_minutes"),
    F.datediff(F.col("flight_date_dt"), F.col("search_date_dt")).alias("search_to_flight_days"),
    # Partition Columns
    F.year(F.col("flight_date_dt")).alias("year"),
    F.month(F.col("flight_date_dt")).alias("month"),
    F.dayofmonth(F.col("flight_date_dt")).alias("day")
)

# 写入最终的事实表，使用动态分区
print("正在将数据写入事实表 fact_flight_ticket...")
final_fact_df.write.mode("overwrite").insertInto(f"{db_name}.fact_flight_ticket")
print("事实表加载完成。")
    # ==========================================================================
    # 5. 清理与关闭
    # ==========================================================================
    print("阶段 5: 清理缓存并关闭Spark Session...")
    final_segments_df.unpersist()
    dim_date_df.unpersist()
    dim_airport_df.unpersist()
    dim_airline_df.unpersist()
    dim_aircraft_df.unpersist()
    dim_flight_details_df.unpersist()

    spark.stop()
    print("ETL流程已成功完成！")


if __name__ == '__main__':
    main()