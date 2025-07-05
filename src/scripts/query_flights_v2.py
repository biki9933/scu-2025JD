# query_from_args_hdfs_path.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import traceback
import argparse

def parse_arguments():
    """
    使用 argparse 解析命令行参数。
    """
    parser = argparse.ArgumentParser(description="根据指定的参数查询中转航班。")
    parser.add_argument("--search_date", required=True, help="搜索日期 (格式 YYYY-MM-DD)")
    parser.add_argument("--flight_date", required=True, help="航班日期 (格式 YYYY-MM-DD)")
    parser.add_argument("--start", required=True, help="出发机场三字码 (例如 SFO)")
    parser.add_argument("--dest", required=True, help="目的机场三字码 (例如 PHL)")
    args = parser.parse_args()
    return args

def main():
    """
    根据命令行传入的参数，从HDFS物理分区路径读取数据，查询特定航线的所有中转方案。
    """
    # --- 1. 获取命令行参数 ---
    args = parse_arguments()
    user_search_date = args.search_date
    user_flight_date = args.flight_date
    user_start_airport = args.start.upper()
    user_final_destination = args.dest.upper()

    spark = SparkSession.builder \
        .appName(f"HDFS Path Query: {user_start_airport} to {user_final_destination}") \
        .getOrCreate()

    print("\n--- 开始在集群模式下查询中转方案 (读取HDFS物理路径) ---")
    
    base_df = None
    try:
        # --- 3. 动态构建HDFS分区路径并读取数据 ---
        
        # 定义你的数据表在HDFS上的根路径
        base_hdfs_path = "/user/hive/warehouse/flight_dw.db/wide_flight_analytics"
        
        # 根据用户输入构建完整的分区路径
        target_partition_path = f"{base_hdfs_path}/search_date={user_search_date}/flight_date={user_flight_date}/"
        
        print(f"INFO: 正在直接读取HDFS分区路径: {target_partition_path}")
        
        try:
            # 使用 spark.read.parquet() 直接读取路径
            all_data_in_partition_df = spark.read.parquet(target_partition_path)
        except Exception as read_error:
            # 捕获路径不存在的错误
            if "Path does not exist" in str(read_error):
                print(f"\n错误: 指定的分区路径不存在: {target_partition_path}")
                print("请检查输入的日期是否正确，以及该分区是否确实有数据。")
                return
            else:
                # 如果是其他读取错误，则重新抛出
                raise read_error

        # --- 4. 在已加载的分区数据上进行过滤 ---
        # 注意：我们不再需要在where子句中过滤日期，因为读取路径已经保证了这一点。
        base_df = all_data_in_partition_df.where(
            (F.col("journey_start_airport") == user_start_airport) &
            (F.col("journey_final_destination") == user_final_destination) &
            (F.col("is_non_stop") == False)
        )
        
        base_df.cache()
        count = base_df.count()
        if count == 0:
            print("\n结果: 在该分区内，未找到符合条件的中转航班记录。")
            return
            
        print(f"\n[步骤1] 过滤成功，找到 {count} 条航段记录。")

        # 5. 行程重构
        print("[步骤2] 开始重构行程...")
        reconstructed_df = base_df.groupBy("leg_id").agg(
            F.sort_array(F.collect_list(F.struct(
                "segment_index", "segment_departure_airport", "segment_arrival_airport",
                "segment_departure_time_raw", "segment_arrival_time_raw", "segment_duration_seconds"
            ))).alias("segments_info"),
            F.first("total_fare").alias("total_fare")
        )

        # 6. 格式化输出
        print("[步骤3] 格式化输出...")
        final_output_df = reconstructed_df.select(
            F.col("leg_id"), F.col("total_fare").alias("总价"),
            F.concat_ws(" -> ", F.expr("transform(segments_info, x -> x.segment_departure_airport)"),
                        F.col("segments_info")[F.size(F.col("segments_info")) - 1]["segment_arrival_airport"]
            ).alias("完整路径"),
            F.expr("aggregate(segments_info, 0D, (acc, x) -> acc + x.segment_duration_seconds) / 60"
            ).cast(IntegerType()).alias("总飞行分钟"),
            F.concat_ws(" | ", F.expr("transform(segments_info, x -> concat(x.segment_departure_time_raw, ' -- ', x.segment_arrival_time_raw))")
            ).alias("航段时间线 (出发 -- 到达)")
        ).orderBy("总价")

        # 7. 展示结果
        print("\n--- 查询结果：所有中转方案 ---")
        final_output_df.show(truncate=False)


    except Exception as e:
        traceback.print_exc()
    
    finally:
        if base_df is not None and base_df.is_cached:
            base_df.unpersist()
        spark.stop()


if __name__ == '__main__':
    main()
