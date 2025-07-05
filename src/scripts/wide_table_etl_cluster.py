# wide_table_etl_cluster.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, BooleanType, DateType, LongType
import traceback

def main():
    """
    1. 读取Hive中的原始数据表。
    2. 拆分多航段字段，包括精确的时间字段。
    3. 生成一个精简的、分区的“宽表”。
    """
    # --- 1. 配置 ---
    source_table = "flight_dw.itineraries_raw_sample"  # 你的原始数据表名
    target_table = "flight_dw.wide_flight_analytics"   # 最终宽表的名称

    # --- 2. 初始化 Spark Session (为集群环境配置) ---
    spark = SparkSession.builder \
        .appName("Wide Flight Table ETL") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

    print("--- 开始ETL流程：创建最终分析宽表 ---")
    print(f"INFO: Spark Version: {spark.version}")
    print(f"INFO: 读取原始数据源: {source_table}")

    try:
        # --- 3. 读取原始数据并进行初步类型转换 ---
        raw_df = spark.table(source_table)
        print("INFO: 原始数据加载成功。")

        typed_df = raw_df.withColumn(
            "search_date_dt", F.to_date(F.col("search_date"), "yyyy-MM-dd")
        ).withColumn(
            "flight_date_dt", F.to_date(F.col("flight_date"), "yyyy-MM-dd")
        ).withColumn(
            "is_non_stop_bool", F.col("is_non_stop").cast(BooleanType())
        ).withColumn(
            "total_fare_num", F.col("total_fare").cast(FloatType())
        )
        print("INFO: 基础类型转换完成。")

        # --- 4. 拆分所有 '||' 分隔的多航段字段为数组 ---
        split_df = typed_df.withColumn(
            "segments_departure_airport_arr", F.split(F.col("segments_departure_airport"), "\|\|")
        ).withColumn(
            "segments_arrival_airport_arr", F.split(F.col("segments_arrival_airport"), "\|\|")
        ).withColumn(
            "segments_airline_name_arr", F.split(F.col("segments_airline_name"), "\|\|")
        ).withColumn(
            "segments_duration_in_seconds_arr", F.split(F.col("segments_duration_in_seconds"), "\|\|")
        ).withColumn(
            "segments_departure_time_raw_arr", F.split(F.col("segments_departure_time_raw"), "\|\|")
        ).withColumn(
            "segments_arrival_time_raw_arr", F.split(F.col("segments_arrival_time_raw"), "\|\|")
        ).withColumn(
            "segments_departure_epoch_arr", F.split(F.col("segments_departure_time_epoch_seconds"), "\|\|")
        ).withColumn(
            "segments_arrival_epoch_arr", F.split(F.col("segments_arrival_time_epoch_seconds"), "\|\|")
        )
        print("INFO: 多航段字段拆分完成。")

        # --- 5. 预计算行程上下文信息 ---
        context_df = split_df.withColumn(
            "total_segments", F.size(F.col("segments_departure_airport_arr"))
        ).withColumn(
            "journey_start_airport", F.col("starting_airport")
        ).withColumn(
            "journey_final_destination", F.col("destination_airport")
        )

        # --- 6. 使用 posexplode 扁平化数据 ---
        exploded_df = context_df.select(
            "*",
            F.posexplode(
                F.arrays_zip(
                    "segments_departure_airport_arr", "segments_arrival_airport_arr",
                    "segments_airline_name_arr", "segments_duration_in_seconds_arr",
                    "segments_departure_time_raw_arr", "segments_arrival_time_raw_arr",
                    "segments_departure_epoch_arr", "segments_arrival_epoch_arr"
                )
            ).alias("pos", "segment_data")
        )
        print("INFO: 数据扁平化完成。")

        # --- 7. 最终选择和重命名列，形成宽表 ---
        final_df = exploded_df.select(
            # 分区键
            F.col("search_date_dt").alias("search_date"),
            F.col("flight_date_dt").alias("flight_date"),
            # ID与索引
            F.col("leg_id"),
            (F.col("pos") + 1).alias("segment_index"),
            # 行程上下文
            F.col("journey_start_airport"),
            F.col("journey_final_destination"),
            F.col("total_fare_num").alias("total_fare"),
            F.col("is_non_stop_bool").alias("is_non_stop"),
            F.col("total_segments"),
            # 航段属性
            F.col("segment_data.segments_departure_airport_arr").alias("segment_departure_airport"),
            F.col("segment_data.segments_arrival_airport_arr").alias("segment_arrival_airport"),
            F.col("segment_data.segments_airline_name_arr").alias("segment_airline_name"),
            F.col("segment_data.segments_duration_in_seconds_arr").cast(IntegerType()).alias("segment_duration_seconds"),
            # 精确时间字段
            F.col("segment_data.segments_departure_time_raw_arr").alias("segment_departure_time_raw"),
            F.col("segment_data.segments_arrival_time_raw_arr").alias("segment_arrival_time_raw"),
            F.col("segment_data.segments_departure_epoch_arr").cast(LongType()).alias("segment_departure_epoch"),
            F.col("segment_data.segments_arrival_epoch_arr").cast(LongType()).alias("segment_arrival_epoch")
        ).where(F.col("search_date").isNotNull() & F.col("flight_date").isNotNull())
        print("INFO: 最终Schema构建完成。")
        
        # --- 8. 将处理好的数据写入新的分区表中 ---
        print(f"INFO: 写入目标分区表: {target_table}")
        final_df.write \
            .partitionBy("search_date", "flight_date") \
            .mode("overwrite") \
            .format("parquet") \
            .option("compression", "snappy") \
            .saveAsTable(target_table)

        print("\nETL流程执行完毕！---")
        print(f"最终的宽表 '{target_table}' 已创建/更新。")

    except Exception as e:

        traceback.print_exc()
    
    finally:

        spark.stop()


if __name__ == '__main__':
    main()