
import datetime
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

# --- 1. 初始化 Spark Session ---
print("INFO: 初始化 Spark Session...")
spark = SparkSession.builder \
    .appName("BatchFlightPrediction_Corrected") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("USE flight_dw")
print("INFO: 已切换到数据库: flight_dw")


# --- 2. 生成未来预测场景 ---
print("INFO: 开始生成未来预测场景...")

# 获取所有需要预测的航线
try:
    routes_df = spark.table("flight_dw.route_min_price_rule").select("departure_airport_fk", "arrival_airport_fk")
except Exception as e:
    spark.stop()
    exit(1)

today = datetime.date.today()
# 生成未来6个月（180天），每5天一个的日期序列
date_list = [(today + datetime.timedelta(days=i)) for i in range(1, 181, 5)]

# 将每个日期对象包装成一个带有明确字段名 'date' 的 Row 对象
DateRow = Row("date")
date_rows = [DateRow(d) for d in date_list]

# 从 Row 对象列表创建DataFrame
dates_df = spark.createDataFrame(date_rows)

# 将航线和日期进行交叉连接
future_scenarios = routes_df.crossJoin(dates_df)

# 为这些场景计算出模型需要的所有特征
future_scenarios = future_scenarios.withColumn("search_to_flight_days", F.datediff(F.col("date"), F.current_date()))
future_scenarios = future_scenarios.withColumn("flight_day_of_week", F.dayofweek(F.col("date")))
future_scenarios = future_scenarios.withColumn("flight_month", F.month(F.col("date")))
future_scenarios = future_scenarios.withColumn("flight_week_of_year", F.weekofyear(F.col("date")))
future_scenarios = future_scenarios.withColumn("airline_fk", F.lit(None).cast("int"))
future_scenarios = future_scenarios.withColumn("aircraft_fk", F.lit(None).cast("int"))

print(f"INFO: 已生成 {future_scenarios.count()} 条未来场景用于预测。")


# --- 3. 加载模型和规则 ---
print("INFO: 开始加载已训练的模型和规则表...")
try:
    model_path = "hdfs:///data/models/flight_price_gbt_model_lightweight_v1"
    model = PipelineModel.load(model_path)
    rules_df = spark.table("flight_dw.route_min_price_rule")
    print("INFO: 模型和规则加载成功！")
except Exception as e:
    print(f"ERROR: 加载模型或规则失败: {e}")
    spark.stop()
    exit(1)

# --- 4. 执行预测并应用规则 ---
print("INFO: 开始执行批量预测...")
predictions = model.transform(future_scenarios)
print("INFO: 批量预测完成。")

final_results = predictions.join(
    rules_df,
    on=["departure_airport_fk", "arrival_airport_fk"],
    how="left"
)
final_results = final_results.withColumn(
    "recommendation",
    F.when(F.col("prediction") <= F.col("best_buy_threshold"), 1).otherwise(0)
)
print("INFO: 购买建议已生成。")


# --- 5. 存储最终结果 ---
print("INFO: 开始将最终结果写入新表...")
output_df = final_results.select(
    F.col("date").alias("flight_date"),
    F.col("departure_airport_fk"),
    F.col("arrival_airport_fk"),
    F.round(F.col("prediction"), 2).alias("predicted_fare"),
    F.col("recommendation")
)
output_table_name = "flight_dw.superset_predictions"
output_df.write.mode("overwrite").saveAsTable(output_table_name)
print(f"SUCCESS: 最终结果已成功写入到 {output_table_name} 表中！")
print("项目端到端流程已全部完成！")


# --- 6. 关闭 Spark Session ---
spark.stop()
