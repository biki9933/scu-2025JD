
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, lit

def search_one_transfer_df(flights_df, origin, destination, dep_date, min_conn, max_conn):
    first_leg = flights_df.filter(col("departure_airport_code") == origin) \
        .withColumnRenamed("arrival_airport_code", "hub_airport") \
        .withColumnRenamed("arrival_time", "arr_time1") \
        .withColumnRenamed("departure_time", "dep_time1") \
        .withColumnRenamed("leg_id", "leg1") \
        .withColumnRenamed("total_fare", "fare1") \
        .withColumnRenamed("total_duration_in_minutes", "dur1") \
        .select("leg1", "departure_airport_code", "hub_airport", "dep_time1", "arr_time1", "fare1", "dur1")

    second_leg = flights_df \
        .withColumnRenamed("departure_airport_code", "hub_airport") \
        .withColumnRenamed("arrival_airport_code", "final_airport") \
        .withColumnRenamed("departure_time", "dep_time2") \
        .withColumnRenamed("arrival_time", "arr_time2") \
        .withColumnRenamed("leg_id", "leg2") \
        .withColumnRenamed("total_fare", "fare2") \
        .withColumnRenamed("total_duration_in_minutes", "dur2") \
        .select("leg2", "hub_airport", "final_airport", "dep_time2", "arr_time2", "fare2", "dur2")

    join_cond = (
        (first_leg.hub_airport == second_leg.hub_airport) &
        (unix_timestamp("dep_time2") - unix_timestamp("arr_time1") >= min_conn * 60) &
        (unix_timestamp("dep_time2") - unix_timestamp("arr_time1") <= max_conn * 60)
    )

    joined = first_leg.join(second_leg, join_cond)

    result = joined.filter(
        (col("final_airport") == destination) &
        (col("departure_airport_code") != col("final_airport")) &
        (col("departure_airport_code") != col("hub_airport")) &
        (col("hub_airport") != col("final_airport"))
    )

    result = result.withColumn("route_type", lit("1-transfer")) \
        .withColumn("total_fare", col("fare1") + col("fare2")) \
        .withColumn("total_duration", col("dur1") + col("dur2")) \
        .select("route_type", "leg1", "leg2", "total_fare", "total_duration")

    return result

def search_two_transfer_df(flights_df, origin, destination, dep_date, min_conn, max_conn):
    leg1 = flights_df.filter(col("departure_airport_code") == origin) \
        .withColumnRenamed("arrival_airport_code", "hub1") \
        .withColumnRenamed("departure_time", "dep1") \
        .withColumnRenamed("arrival_time", "arr1") \
        .withColumnRenamed("leg_id", "leg1") \
        .withColumnRenamed("total_fare", "fare1") \
        .withColumnRenamed("total_duration_in_minutes", "dur1") \
        .select("leg1", "departure_airport_code", "hub1", "dep1", "arr1", "fare1", "dur1")

    leg2 = flights_df \
        .withColumnRenamed("departure_airport_code", "hub1") \
        .withColumnRenamed("arrival_airport_code", "hub2") \
        .withColumnRenamed("departure_time", "dep2") \
        .withColumnRenamed("arrival_time", "arr2") \
        .withColumnRenamed("leg_id", "leg2") \
        .withColumnRenamed("total_fare", "fare2") \
        .withColumnRenamed("total_duration_in_minutes", "dur2") \
        .select("leg2", "hub1", "hub2", "dep2", "arr2", "fare2", "dur2")

    mid_join1 = leg1.join(leg2,
        (leg1.hub1 == leg2.hub1) &
        (unix_timestamp("dep2") - unix_timestamp("arr1") >= min_conn * 60) &
        (unix_timestamp("dep2") - unix_timestamp("arr1") <= max_conn * 60)
    )

    leg3 = flights_df \
        .withColumnRenamed("departure_airport_code", "hub2") \
        .withColumnRenamed("arrival_airport_code", "final_airport") \
        .withColumnRenamed("departure_time", "dep3") \
        .withColumnRenamed("arrival_time", "arr3") \
        .withColumnRenamed("leg_id", "leg3") \
        .withColumnRenamed("total_fare", "fare3") \
        .withColumnRenamed("total_duration_in_minutes", "dur3") \
        .select("leg3", "hub2", "final_airport", "dep3", "arr3", "fare3", "dur3")

    mid_join2 = mid_join1.join(leg3,
        (mid_join1.hub2 == leg3.hub2) &
        (unix_timestamp("dep3") - unix_timestamp("arr2") >= min_conn * 60) &
        (unix_timestamp("dep3") - unix_timestamp("arr2") <= max_conn * 60)
    )

    result = mid_join2.filter(
        (col("final_airport") == destination) &
        (col("departure_airport_code") != col("hub1")) &
        (col("departure_airport_code") != col("hub2")) &
        (col("departure_airport_code") != col("final_airport")) &
        (col("hub1") != col("hub2")) &
        (col("hub1") != col("final_airport")) &
        (col("hub2") != col("final_airport"))
    )

    result = result.withColumn("route_type", lit("2-transfer")) \
        .withColumn("total_fare", col("fare1") + col("fare2") + col("fare3")) \
        .withColumn("total_duration", col("dur1") + col("dur2") + col("dur3")) \
        .select("route_type", "leg1", "leg2", "leg3", "total_fare", "total_duration")

    return result

if __name__ == "__main__":
    import sys
    spark = SparkSession.builder.appName("Flight Recommendation").enableHiveSupport().getOrCreate()

    origin = sys.argv[1]
    destination = sys.argv[2]
    dep_date = sys.argv[3]
    output_path = sys.argv[4]

    min_conn = 45   # minutes
    max_conn = 720  # 12 hours

    flights_df = spark.sql(f"SELECT * FROM fact_flight_ticket WHERE year = year('{dep_date}') AND month = month('{dep_date}') AND day = day('{dep_date}')")

    one_df = search_one_transfer_df(flights_df, origin, destination, dep_date, min_conn, max_conn)
    two_df = search_two_transfer_df(flights_df, origin, destination, dep_date, min_conn, max_conn)

    result_df = one_df.unionByName(two_df)
    result_df.write.mode("overwrite").json(output_path)
    spark.stop()
