# -*- coding: utf-8 -*-
#
# ==============================================================================
# 项目: 机票元搜索及智能推荐 - 数据仓库ETL脚本 (修正版)
#
# 修正内容:
#   1. 修正了列名不匹配的问题
#   2. 添加了数据质量检查
#   3. 修正了JOIN条件和字段映射
#   4. 添加了详细的调试信息
# ==============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def main():
    """
    主ETL处理函数 - 修正版
    """
    # ==========================================================================
    # 0. 初始化与配置
    # ==========================================================================

    # 【请修改】设置您的Hive数据库名称
    db_name = "flight_dw"

    # 初始化支持Hive的SparkSession
    spark = SparkSession.builder \
        .appName("FlightDW_ETL_Fixed_v3") \
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
    itineraries_raw_df = spark.table(f"{db_name}.itineraries_raw_sample")
    airports_raw_df = spark.table(f"{db_name}.raw_airports")

    # 【调试】打印原始数据结构
    print("原始行程数据结构:")
    itineraries_raw_df.printSchema()
    print("原始机场数据结构:")
    airports_raw_df.printSchema()

    print("原始数据加载完成。")

    # ==========================================================================
    # 2. 转换 (Transform): 航段"爆炸"与预处理 (修正后的核心逻辑)
    # ==========================================================================
    print("阶段 2: 正在处理多航段行程数据 (爆炸操作)...")

    # 【修正】使用正确的列名 - 基于您提供的数据结构
    split_cols = [
        'segmentsDepartureTimeEpochSeconds', 'segmentsDepartureTimeRaw',
        'segmentsArrivalTimeEpochSeconds', 'segmentsArrivalTimeRaw',
        'segmentsArrivalAirportCode', 'segmentsDepartureAirportCode',
        'segmentsAirlineName', 'segmentsAirlineCode',
        'segmentsEquipmentDescription', 'segmentsDurationInSeconds',
        'segmentsDistance', 'segmentsCabinCode'
    ]

    segments_df = itineraries_raw_df

    # 检查每个字段是否存在
    existing_cols = itineraries_raw_df.columns
    for col_name in split_cols:
        if col_name not in existing_cols:
            print(f"警告: 字段 {col_name} 不存在于原始数据中")
            print(f"可用字段: {existing_cols}")
            continue
        segments_df = segments_df.withColumn(f"{col_name}_arr", F.split(F.col(col_name), "\\|\\|"))

    # 使用存在的字段进行爆炸操作
    if 'segmentsDepartureAirportCode' in existing_cols:
        exploded_df = segments_df.select(
            "*", F.posexplode(F.col("segmentsDepartureAirportCode_arr")).alias("pos", "segment_departure_code")
        )
    else:
        print("错误: 无法找到航段出发机场代码字段")
        return

    # 【修正】构建最终的航段数据框，使用正确的字段名
    final_segments_df = exploded_df.select(
        F.col("legId").alias("leg_id"),  # 修正字段名
        F.to_date(F.col("searchDate"), "yyyy/M/d").alias("search_date_dt"),  # 修正日期格式
        F.to_date(F.col("flightDate"), "yyyy/M/d").alias("flight_date_dt"),  # 修正日期格式
        F.col("isRefundable").cast("boolean").alias("is_refundable"),  # 修正字段名
        F.col("isBasicEconomy").cast("boolean").alias("is_basic_economy"),  # 修正字段名
        F.col("isNonStop").cast("boolean").alias("is_non_stop"),  # 修正字段名
        F.col("baseFare").cast("double").alias("base_fare"),  # 修正字段名
        F.col("totalFare").cast("double").alias("total_fare"),  # 修正字段名
        F.col("seatsRemaining").cast("int").alias("seats_remaining"),  # 修正字段名
        F.col("totalTravelDistance").cast("int").alias("total_travel_distance"),  # 修正字段名
        (F.col("pos") + 1).alias("segment_order"),
        F.col("segment_departure_code"),
        F.col("segmentsArrivalAirportCode_arr")[F.col("pos")].alias("segment_arrival_code"),
        F.col("segmentsAirlineName_arr")[F.col("pos")].alias("airline_name"),
        F.col("segmentsAirlineCode_arr")[F.col("pos")].alias("airline_code"),
        F.trim(F.col("segmentsEquipmentDescription_arr")[F.col("pos")]).alias("equipment_description"),
        F.col("segmentsDurationInSeconds_arr")[F.col("pos")].cast("int").alias("segment_duration_in_seconds")
    ).filter(F.col("flight_date_dt").isNotNull())

    final_segments_df.cache()

    # 【调试】打印处理后的数据样本
    print("处理后的航段数据样本:")
    final_segments_df.show(5, truncate=False)
    print(f"总航段数: {final_segments_df.count()}")

    # ==========================================================================
    # 3. 转换与加载 (Transform & Load): 创建并加载所有维度表
    # ==========================================================================
    print("阶段 3: 正在创建并加载所有维度表...")

    # --- 3.1 dim_date ---
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

    # 【调试】检查日期维度数据
    print("日期维度数据样本:")
    dim_date_df.show(5)

    dim_date_df.write.mode("overwrite").insertInto(f"{db_name}.dim_date")
    dim_date_df.cache()
    print(f"  dim_date 加载完成. 共 {dim_date_df.count()} 条记录.")

    # --- 3.2 dim_airport ---
    print("  正在处理 dim_airport...")
    # 【修正】过滤掉空的机场代码
    dim_airport_df = (airports_raw_df.select(
        F.col("iata_code").alias("airport_code"),
        F.col("name").alias("airport_name"),
        F.col("municipality").alias("city"),
        F.col("iso_country").alias("country_code")
    ).filter(
        (F.col("airport_code").isNotNull()) &
        (F.col("airport_code") != "") &
        (F.col("airport_code") != "\\N")  # 过滤掉NULL值的字符串表示
    ).distinct()
                      .withColumn("airport_fk", F.monotonically_increasing_id().cast("INT"))
                      .select("airport_fk", "airport_code", "airport_name", "city", "country_code")
                      )

    # 【调试】检查机场维度数据
    print("机场维度数据样本:")
    dim_airport_df.show(5, truncate=False)

    dim_airport_df.write.mode("overwrite").insertInto(f"{db_name}.dim_airport")
    dim_airport_df.cache()
    print(f"  dim_airport 加载完成. 共 {dim_airport_df.count()} 条记录.")

    # --- 3.3 dim_airline ---
    print("  正在处理 dim_airline...")
    dim_airline_df = (final_segments_df.select("airline_code", "airline_name")
                      .filter(
        (F.col("airline_code").isNotNull()) &
        (F.col("airline_code") != "") &
        (F.col("airline_name").isNotNull()) &
        (F.col("airline_name") != "")
    ).distinct()
                      .withColumn("alliance_name", F.lit(None).cast(StringType()))
                      .withColumn("airline_fk", F.monotonically_increasing_id().cast("INT"))
                      .select("airline_fk", "airline_code", "airline_name", "alliance_name")
                      )

    # 【调试】检查航司维度数据
    print("航司维度数据样本:")
    dim_airline_df.show(5, truncate=False)

    dim_airline_df.write.mode("overwrite").insertInto(f"{db_name}.dim_airline")
    dim_airline_df.cache()
    print(f"  dim_airline 加载完成. 共 {dim_airline_df.count()} 条记录.")

    # --- 3.4 dim_aircraft (动态创建) ---
    print("  正在处理 dim_aircraft...")
    dim_aircraft_df = (final_segments_df.select(F.trim(F.col("equipment_description")).alias("equipment_description"))
                       .filter(
        (F.col("equipment_description").isNotNull()) &
        (F.col("equipment_description") != "") &
        (F.col("equipment_description") != "\\N")
    )
                       .distinct()
                       .withColumn("manufacturer", F.lit(None).cast(StringType()))
                       .withColumn("aircraft_fk", F.monotonically_increasing_id().cast("INT"))
                       .select("aircraft_fk", "equipment_description", "manufacturer")
                       )

    # 【调试】检查机型维度数据
    print("机型维度数据样本:")
    dim_aircraft_df.show(5, truncate=False)

    dim_aircraft_df.write.mode("overwrite").insertInto(f"{db_name}.dim_aircraft")
    dim_aircraft_df.cache()
    print(f"  dim_aircraft 加载完成. 共 {dim_aircraft_df.count()} 条记录.")

    # --- 3.5 dim_flight_details (垃圾维度) ---
    print("  正在处理 dim_flight_details...")
    dim_flight_details_df = (final_segments_df.select("is_refundable", "is_basic_economy", "is_non_stop")
                             .distinct()
                             .withColumn("flight_number", F.lit(None).cast(StringType()))
                             .withColumn("flight_details_fk", F.monotonically_increasing_id().cast("INT"))
                             .select("flight_details_fk", "flight_number", "is_refundable", "is_basic_economy",
                                     "is_non_stop")
                             )

    # 【调试】检查航班详情维度数据
    print("航班详情维度数据样本:")
    dim_flight_details_df.show(5, truncate=False)

    dim_flight_details_df.write.mode("overwrite").insertInto(f"{db_name}.dim_flight_details")
    dim_flight_details_df.cache()
    print(f"  dim_flight_details 加载完成. 共 {dim_flight_details_df.count()} 条记录.")

    # ==========================================================================
    # 4. 加载 (Load): 创建并加载事实表 (修正JOIN逻辑)
    # ==========================================================================
    print("阶段 4: 正在关联维度表并生成最终的事实表...")

    # 【调试】检查JOIN前的关键字段
    print("检查最终航段数据中的机场代码:")
    final_segments_df.select("segment_departure_code", "segment_arrival_code").distinct().show(10)

    print("检查机场维度表中的机场代码:")
    dim_airport_df.select("airport_code").distinct().show(10)

    # 【修正】使用正确的JOIN条件和字段处理
    fact_joined_df = final_segments_df \
        .join(dim_date_df.alias("search_dt"), F.col("search_date_dt") == F.col("search_dt.full_date"), "left") \
        .join(dim_date_df.alias("flight_dt"), F.col("flight_date_dt") == F.col("flight_dt.full_date"), "left") \
        .join(dim_airline_df,
              (F.col("airline_code") == F.col("dim_airline_df.airline_code")) &
              (F.col("airline_name") == F.col("dim_airline_df.airline_name")), "left") \
        .join(dim_airport_df.alias("dep_ap"),
              F.col("segment_departure_code") == F.col("dep_ap.airport_code"), "left") \
        .join(dim_airport_df.alias("arr_ap"),
              F.col("segment_arrival_code") == F.col("arr_ap.airport_code"), "left") \
        .join(dim_aircraft_df,
              F.col("equipment_description") == F.col("dim_aircraft_df.equipment_description"), "left") \
        .join(dim_flight_details_df,
              (F.col("is_refundable") == F.col("dim_flight_details_df.is_refundable")) &
              (F.col("is_basic_economy") == F.col("dim_flight_details_df.is_basic_economy")) &
              (F.col("is_non_stop") == F.col("dim_flight_details_df.is_non_stop")), "left")

    # 【修正】处理NULL值和构建最终事实表
    final_fact_df = fact_joined_df.select(
        F.coalesce(F.col("search_dt.date_fk"), F.lit(-1)).alias("search_date_fk"),
        F.coalesce(F.col("flight_dt.date_fk"), F.lit(-1)).alias("flight_date_fk"),
        F.coalesce(F.col("airline_fk"), F.lit(-1)).alias("airline_fk"),
        F.coalesce(F.col("dep_ap.airport_fk"), F.lit(-1)).alias("departure_airport_fk"),
        F.coalesce(F.col("arr_ap.airport_fk"), F.lit(-1)).alias("arrival_airport_fk"),
        F.coalesce(F.col("aircraft_fk"), F.lit(-1)).alias("aircraft_fk"),
        F.coalesce(F.col("flight_details_fk"), F.lit(-1)).alias("flight_details_fk"),
        F.col("leg_id"),
        F.col("base_fare"),
        F.col("total_fare"),
        F.col("seats_remaining"),
        F.col("total_travel_distance"),
        (F.col("segment_duration_in_seconds") / 60).cast("int").alias("total_duration_in_minutes"),
        F.datediff(F.col("flight_date_dt"), F.col("search_date_dt")).alias("search_to_flight_days"),
        F.year(F.col("flight_date_dt")).alias("year"),
        F.month(F.col("flight_date_dt")).alias("month"),
        F.dayofmonth(F.col("flight_date_dt")).alias("day")
    )

    # 【调试】检查最终事实表数据
    print("最终事实表数据样本:")
    final_fact_df.show(10, truncate=False)

    print("正在将数据写入事实表 fact_flight_ticket...")
    final_fact_df.write.mode("overwrite").insertInto(f"{db_name}.fact_flight_ticket")
    print("事实表加载完成。")

    # ==========================================================================
    # 5. 数据质量检查
    # ==========================================================================
    print("阶段 5: 数据质量检查...")

    # 检查外键连接成功率
    total_records = final_fact_df.count()

    airline_success = final_fact_df.filter(F.col("airline_fk") != -1).count()
    airport_dep_success = final_fact_df.filter(F.col("departure_airport_fk") != -1).count()
    airport_arr_success = final_fact_df.filter(F.col("arrival_airport_fk") != -1).count()
    aircraft_success = final_fact_df.filter(F.col("aircraft_fk") != -1).count()

    print(f"总记录数: {total_records}")
    print(f"航司外键连接成功率: {airline_success / total_records * 100:.2f}%")
    print(f"出发机场外键连接成功率: {airport_dep_success / total_records * 100:.2f}%")
    print(f"到达机场外键连接成功率: {airport_arr_success / total_records * 100:.2f}%")
    print(f"机型外键连接成功率: {aircraft_success / total_records * 100:.2f}%")

    # ==========================================================================
    # 6. 清理与关闭
    # ==========================================================================
    print("阶段 6: 清理缓存并关闭Spark Session...")
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