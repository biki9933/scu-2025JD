# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, BooleanType, DateType
)


def main():
    """
    完整的机票数据ETL流程:
    1. 读取原始数据
    2. 类型转换与数据清洗
    3. 多航段拆分与行程上下文生成
    4. 按 (search_date, flight_date) 分区写入Parquet表
    """
    # --- 1. 初始化与配置 ---
    # 在生产环境中，可以从配置文件读取这些参数
    source_table = "flight_dw.itineraries_raw_sample"  # 你的原始数据表名
    target_table = "flight_dw.fact_flight_segments"  # 目标事实表名

    spark = SparkSession.builder \
        .appName("Flight Itineraries ETL") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    print("INFO: Spark Session 已启动。")
    print(f"INFO: 读取原始数据源: {source_table}")

    # --- 2. 读取原始数据并进行初步类型转换 ---
    # 假设原始数据表的所有列都是STRING类型
    raw_df = spark.table(source_table)

    # 对非航段、非数组的列进行类型转换
    print("INFO: 开始对行程级别字段进行类型转换...")
    typed_df = raw_df.withColumn(
        "search_date_dt", F.to_date(F.col("search_date"), "yyyy-MM-dd")
    ).withColumn(
        "flight_date_dt", F.to_date(F.col("flight_date"), "yyyy-MM-dd")
    ).withColumn(
        "elapsed_days_num", F.col("elapsed_days").cast(IntegerType())
    ).withColumn(
        "is_basic_economy_bool", F.col("is_basic_economy").cast(BooleanType())
    ).withColumn(
        "is_refundable_bool", F.col("is_refundable").cast(BooleanType())
    ).withColumn(
        "is_non_stop_bool", F.col("is_non_stop").cast(BooleanType())
    ).withColumn(
        "base_fare_num", F.col("base_fare").cast(FloatType())
    ).withColumn(
        "total_fare_num", F.col("total_fare").cast(FloatType())
    ).withColumn(
        "seats_remaining_num", F.col("seats_remaining").cast(IntegerType())
    ).withColumn(
        "total_travel_distance_num", F.col("total_travel_distance").cast(IntegerType())
    )

    # --- 3. 【核心】拆分多航段字段为数组 ---
    print("INFO: 开始拆分 '||' 分隔的多航段字段...")
    # 注意 '||' 在split中是正则表达式，需要转义为 '\|\|'
    split_df = typed_df.withColumn(
        "segments_departure_airport_arr", F.split(F.col("segments_departure_airport"), "\|\|")
    ).withColumn(
        "segments_arrival_airport_arr", F.split(F.col("segments_arrival_airport"), "\|\|")
    ).withColumn(
        "segments_airline_name_arr", F.split(F.col("segments_airline_name"), "\|\|")
    ).withColumn(
        "segments_duration_in_seconds_arr", F.split(F.col("segments_duration_in_seconds"), "\|\|")
    ).withColumn(
        "segments_distance_arr", F.split(F.col("segments_distance"), "\|\|")
    ).withColumn(
        "segments_cabin_code_arr", F.split(F.col("segments_cabin_code"), "\|\|")
    ).withColumn(
        "segments_equipment_description_arr", F.split(F.col("segments_equipment_description"), "\|\|")
    )

    # --- 4. 【核心】预计算“行程上下文”信息 ---
    print("INFO: 开始生成行程上下文信息 (total_segments, journey_start_airport, etc.)...")
    context_df = split_df.withColumn(
        "total_segments", F.size(F.col("segments_departure_airport_arr"))
    ).withColumn(
        # 行程起点就是原始表中的 starting_airport
        "journey_start_airport", F.col("starting_airport")
    ).withColumn(
        # 行程终点就是原始表中的 destination_airport
        "journey_final_destination", F.col("destination_airport")
    )

    # --- 5. 【核心】使用 posexplode 爆炸并整理最终字段 ---
    print("INFO: 使用 posexplode 扁平化数据...")
    # 修正点: 使用 .select() 和 .alias("pos", "segment_data") 来处理 posexplode 的两列输出
    exploded_df = context_df.select(
        "*",  # 保留 context_df 中的所有现有列
        F.posexplode(
            F.arrays_zip(
                "segments_departure_airport_arr", "segments_arrival_airport_arr",
                "segments_airline_name_arr", "segments_duration_in_seconds_arr",
                "segments_distance_arr", "segments_cabin_code_arr", "segments_equipment_description_arr"
            )
        ).alias("pos", "segment_data")  # 为两列输出分别提供别名: 'pos' 和 'segment_data'
    )

    # 整理成最终事实表的结构
    # 修正点: 从新的 'pos' 和 'segment_data' 列中提取信息
    final_df = exploded_df.select(
        # === 分区键 (必须放在后面，但先在这里定义) ===
        F.col("search_date_dt").alias("search_date"),
        F.col("flight_date_dt").alias("flight_date"),

        # === 航段自身信息 (Segment Level) ===
        F.col("leg_id"),
        (F.col("pos") + 1).alias("segment_index"),  # 使用新的 'pos' 列
        F.col("segment_data.segments_departure_airport_arr").alias("segment_departure_airport"),
        F.col("segment_data.segments_arrival_airport_arr").alias("segment_arrival_airport"),
        F.col("segment_data.segments_airline_name_arr").alias("segment_airline_name"),
        F.col("segment_data.segments_duration_in_seconds_arr").cast(IntegerType()).alias("segment_duration_seconds"),
        F.col("segment_data.segments_distance_arr").cast(IntegerType()).alias("segment_distance"),
        F.col("segment_data.segments_cabin_code_arr").alias("segment_cabin_code"),
        F.col("segment_data.segments_equipment_description_arr").alias("segment_equipment_description"),

        # === 行程上下文信息 (Itinerary Level) ===
        F.col("total_fare_num").alias("total_fare"),
        F.col("is_non_stop_bool").alias("is_non_stop"),
        F.col("total_segments"),
        F.col("journey_start_airport"),
        F.col("journey_final_destination"),
    ).where(F.col("search_date").isNotNull() & F.col("flight_date").isNotNull())

    # --- 6. 将处理好的数据写入新的分区表中 ---
    print(f"INFO: 开始将数据写入目标分区表: {target_table}")
    final_df.write \
        .partitionBy("search_date", "flight_date") \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(target_table)

    print("SUCCESS: ETL流程执行完毕！")

    # 停止Spark Session
    spark.stop()


if __name__ == '__main__':
    # 只有当这个脚本被直接执行时，才调用main()函数
    main()