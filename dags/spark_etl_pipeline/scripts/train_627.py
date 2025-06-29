from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # 引入functions模块，通常简写为F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# 1. 初始化Spark Session并加载数据
spark = SparkSession.builder \
    .appName("FlightML_FeatureEngineering") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")
spark.sql("USE flight_dw")
print("INFO: 已切换到数据库: flight_dw")

try:
    # 加载您的数据表
    data = spark.table("ml_training_data")
    print("SUCCESS: 特征数据表 'ml表' 加载成功！")
except Exception as e:
    print(f"ERROR: 加载数据表失败，请确保表名正确: {e}")
    spark.stop()
    exit(1)

# 【关键修复】在这里过滤掉 leg_id 为 'legid' 的无效测试数据
# -----------------------------------------------------------
original_count = data.count()
data_cleaned = data.filter(F.col("leg_id") != "legid")
cleaned_count = data_cleaned.count()
print(f"INFO: 已过滤无效数据。原始数据行数: {original_count}, 清理后行数: {cleaned_count}")
# -----------------------------------------------------------

# 【新增修复】在这里删除所有特征列中包含任何空值的行
# -----------------------------------------------------------
final_data = data_cleaned.na.drop()
final_count = final_data.count()
print(f"INFO: 已删除含空值的行。过滤前行数: {cleaned_count}, 最终可用行数: {final_count}")
# -----------------------------------------------------------


# 2. 数据预处理与特征转换 (后续所有步骤都使用 data_cleaned)
# ---------------------------------
# 定义类别特征列和数值特征列
categorical_cols = ["departure_airport_fk", "arrival_airport_fk", "airline_fk", "aircraft_fk", "flight_day_of_week", "flight_month"]
numerical_cols = ["search_to_flight_days", "flight_week_of_year"]

# 创建 StringIndexer 用于将类别列转换为索引数字
indexers = [StringIndexer(inputCol=col, outputCol=col + "_indexed", handleInvalid="keep") for col in categorical_cols]

# 创建 VectorAssembler 用于将所有特征合并成一个向量
assembler_inputs = [col + "_indexed" for col in categorical_cols] + numerical_cols
vector_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

# 3. 定义模型
# ---------------------------------
gbt = GBTRegressor(featuresCol="features", labelCol="total_fare",maxBins=300)

# 4. 构建机器学习流水线 (Pipeline)
# ---------------------------------
pipeline = Pipeline(stages=indexers + [vector_assembler, gbt])

# 5. 拆分数据为训练集和测试集
# ---------------------------------
# 使用清理后的数据 data_cleaned 进行拆分
(training_data, test_data) = final_data.randomSplit([0.8, 0.2], seed=42)
print(f"INFO: 数据已拆分。训练集数量: {training_data.count()}, 测试集数量: {test_data.count()}")

# 6. 训练模型
# ---------------------------------
print("INFO: 开始训练梯度提升树回归模型...")
model = pipeline.fit(training_data)
print("SUCCESS: 模型训练完成！")

# 7. 在测试集上进行预测
# ---------------------------------
predictions = model.transform(test_data)
print("INFO: 已在测试集上生成预测。")
predictions.select("total_fare", "prediction").show(10)

# 8. 评估模型性能
# ---------------------------------
evaluator = RegressionEvaluator(labelCol="total_fare", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"模型评估结果 - 均方根误差 (RMSE): {rmse}")
print("说明: RMSE代表模型预测值与真实值之间的平均差异。这个值越小，说明模型预测得越准。")

# 9. 保存模型（可选但推荐）
# ---------------------------------
model_path = "hdfs:///data/results/flight_fare_gbt_model"
model.save(model_path)
print(f"SUCCESS: 模型已保存到 {model_path}")

spark.stop()