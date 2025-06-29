# flight_etl_final.py (v3 - Complete and Unabridged)
#
# This script performs the full ETL process for the Flight Data Warehouse.
# It reads raw data, handles multi-segment flights by exploding them,
# processes and loads all dimension tables with surrogate keys,
# and finally, loads the fully joined fact table.
#

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    """
    主ETL处理函数
    """
    # 1. 初始化SparkSession，并启用Hive支持
    spark = SparkSession.builder \
        .appName("FlightDW_ETL_Final") \
        .config("spark.sql.exec.dynamicPartition", "true") \
        .config("spark.sql.exec.dynamicPartition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()

    print("Spark Session Created. Starting Final ETL process...")

    # ==============================================================================
    # 阶段一: 加载原始数据
    # ==============================================================================
    itineraries_raw_df = spark.table("flight_dw.itineraries_raw")
    airports_raw_df = spark.table("flight_dw.airports_raw")
    aircraft_raw_df = spark.table("flight_dw.aircraft_raw")

    # ==============================================================================
    # 阶段二: 航段“爆炸”与预处理 (核心逻辑)
    # ==============================================================================
    print("Exploding multi-segment itineraries...")

    split_cols = [
        'segmentsDepartureTimeEpochSeconds', 'segmentsDepartureTimeRaw',
        'segmentsArrivalTimeEpochSeconds', 'segmentsArrivalTimeRaw',
        'segmentsArrivalAirportCode', 'segmentsDepartureAirportCode',
        'segmentsAirlineName', 'segmentsAirlineCode',
        'segmentsEquipmentDescription', 'segmentsDurationInSeconds',
        'segmentsDistance', 'segmentsCabinCode'
    ]

    segments_df = itineraries_raw_df
    for col_name in split_cols:
        segments_df = segments_df.withColumn(f"{col_name}_arr", F.split(F.col(col_name), "\\|\\|"))

    exploded_df = segments_df.select(
        "*", F.posexplode("segmentsDepartureAirportCode_arr").alias("pos", "segment_departure_code")
    )

    final_segments_df = exploded_df.select(
        F.col("legId").alias("leg_id"),
        F.to_date(F.col("searchDate"), "yyyy/M/d").alias("search_date_dt"),
        F.to_date(F.col("flightDate"), "yyyy/M/d").alias("flight_date_dt"),
        F.col("isRefundable").cast("boolean"),
        F.col("isBasicEconomy").cast("boolean"),
        F.col("isNonStop").cast("boolean"),
        F.col("baseFare").cast("double"),
        F.col("totalFare").cast("double"),
        F.col("seatsRemaining").cast("int"),
        F.col("totalTravelDistance").cast("int"),
        (F.col("pos") + 1).alias("segment_order"),
        F.col("segment_departure_code"),
        F.col("segmentsArrivalAirportCode_arr")[F.col("pos")].alias("segment_arrival_code"),
        F.col("segmentsAirlineName_arr")[F.col("pos")].alias("airline_name"),
        F.col("segmentsAirlineCode_arr")[F.col("pos")].alias("airline_code"),
        F.trim(F.col("segmentsEquipmentDescription_arr")[F.col("pos")]).alias("equipment_description"),
        F.col("segmentsDurationInSeconds_arr")[F.col("pos")].cast("int").alias("segment_duration_in_seconds")
    ).filter(F.col("flight_date_dt").isNotNull())

    final_segments_df.cache()
    print("Multi-segment itineraries exploded and pre-processed.")

    # ==============================================================================
    # 阶段三: 创建并加载所有维度表
    # ==============================================================================

    # --- 3.1 dim_date ---
    print("Processing dim_date...")
    date_df = final_segments_df.select(F.col("search_date_dt").alias("full_date")).union(
        final_segments_df.select(F.col("flight_date_dt").alias("full_date"))
    ).distinct().filter(F.col("full_date").isNotNull())

    dim_date_df = date_df.select(
        F.col("full_date"),
        F.year(F.col("full_date")).alias("year_of_date"),
        F.month(F.col("full_date")).alias("month_of_date"),
        F.dayofmonth(F.col("full_date")).alias("day_of_month"),
        F.dayofweek(F.col("full_date")).alias("day_of_week"),
        F.when(F.dayofweek(F.col("full_date")).isin([1, 7]), True).otherwise(False).alias("is_weekend")
    ).withColumn("date_fk", F.monotonically_increasing_id())

    dim_date_df.write.mode("overwrite").insertInto("flight_dw.dim_date")
    dim_date_df.cache()
    print(f"dim_date loaded. Count: {dim_date_df.count()}")

    # --- 3.2 dim_airport ---
    print("Processing dim_airport...")
    dim_airport_df = airports_raw_df.select(
        F.col("iata_code").alias("airport_code"),
        F.col("name").alias("airport_name"),
        F.col("municipality").alias("city"),
        F.col("iso_country").alias("country_code")
    ).filter(F.col("airport_code").isNotNull()).distinct().withColumn("airport_fk", F.monotonically_increasing_id())
    dim_airport_df.write.mode("overwrite").insertInto("flight_dw.dim_airport")
    dim_airport_df.cache()
    print(f"dim_airport loaded. Count: {dim_airport_df.count()}")

    # --- 3.3 dim_airline ---
    print("Processing dim_airline...")
    dim_airline_df = final_segments_df.select("airline_code", "airline_name").filter(
        F.col("airline_code").isNotNull()).distinct() \
        .withColumn("alliance_name", F.lit(None).cast("string")) \
        .withColumn("airline_fk", F.monotonically_increasing_id())
    dim_airline_df.write.mode("overwrite").insertInto("flight_dw.dim_airline")
    dim_airline_df.cache()
    print(f"dim_airline loaded. Count: {dim_airline_df.count()}")

    # --- 3.4 dim_aircraft ---
    print("Processing dim_aircraft...")
    dim_aircraft_df = aircraft_raw_df.select("manufacturer", F.trim(F.col("model")).alias("equipment_description")) \
        .filter(F.col("equipment_description") != "").distinct() \
        .withColumn("aircraft_fk", F.monotonically_increasing_id())
    dim_aircraft_df.write.mode("overwrite").insertInto("flight_dw.dim_aircraft")
    dim_aircraft_df.cache()
    print(f"dim_aircraft loaded. Count: {dim_aircraft_df.count()}")

    # --- 3.5 dim_flight_details ---
    print("Processing dim_flight_details...")
    dim_flight_details_df = final_segments_df.select("is_refundable", "is_basic_economy", "is_non_stop").distinct() \
        .withColumn("flight_number", F.lit(None).cast("string")) \
        .withColumn("flight_details_fk", F.monotonically_increasing_id())
    dim_flight_details_df.write.mode("overwrite").insertInto("flight_dw.dim_flight_details")
    dim_flight_details_df.cache()
    print(f"dim_flight_details loaded. Count: {dim_flight_details_df.count()}")

    # ==============================================================================
    # 阶段四: 加载事实表 (最终关联)
    # ==============================================================================
    print("Processing fact_flight_ticket...")

    fact_joined_df = final_segments_df \
        .join(dim_date_df.alias("search_dt"), F.col("search_date_dt") == F.col("search_dt.full_date"), "left") \
        .join(dim_date_df.alias("flight_dt"), F.col("flight_date_dt") == F.col("flight_dt.full_date"), "left") \
        .join(dim_airline_df, ["airline_code"], "left") \
        .join(dim_airport_df.alias("dep_ap"), F.col("segment_departure_code") == F.col("dep_ap.airport_code"), "left") \
        .join(dim_airport_df.alias("arr_ap"), F.col("segment_arrival_code") == F.col("arr_ap.airport_code"), "left") \
        .join(dim_aircraft_df, ["equipment_description"], "left") \
        .join(dim_flight_details_df, ["is_refundable", "is_basic_economy", "is_non_stop"], "left")

    final_fact_df = fact_joined_df.select(
        F.col("search_dt.date_fk").alias("search_date_fk"),
        F.col("flight_dt.date_fk").alias("flight_date_fk"),
        F.col("airline_fk"),
        F.col("dep_ap.airport_fk").alias("departure_airport_fk"),
        F.col("arr_ap.airport_fk").alias("arrival_airport_fk"),
        F.col("aircraft_fk"),
        F.col("flight_details_fk"),
        F.col("leg_id"),
        F.col("baseFare").alias("base_fare"),
        F.col("totalFare").alias("total_fare"),
        F.col("seatsRemaining").alias("seats_remaining"),
        F.col("totalTravelDistance").alias("total_travel_distance"),
        (F.col("segment_duration_in_seconds") / 60).cast("int").alias("total_duration_in_minutes"),
        F.datediff(F.col("flight_date_dt"), F.col("search_date_dt")).alias("search_to_flight_days")
    ).withColumn("year", F.year(F.col("flight_date_dt"))) \
        .withColumn("month", F.month(F.col("flight_date_dt"))) \
        .withColumn("day", F.dayofmonth(F.col("flight_date_dt")))

    # 写入事实表
    final_fact_df.write.mode("overwrite").insertInto("flight_dw.fact_flight_ticket")

    final_segments_df.unpersist()
    dim_date_df.unpersist()
    dim_airport_df.unpersist()
    dim_airline_df.unpersist()
    dim_aircraft_df.unpersist()
    dim_flight_details_df.unpersist()

    spark.stop()
    print("ETL Process Completed Successfully.")


if __name__ == '__main__':
    main()