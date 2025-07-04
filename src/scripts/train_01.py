# =================================================================
# 文件名: train_price_prediction_model.py
# 描述: 训练票价预测回归模型，并将模型保存到HDFS
# =================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, Imputer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# --- 1. 初始化 Spark Session ---
print("INFO: 初始化 Spark Session...")
spark = SparkSession.builder \
    .appName("FlightPricePredictionTraining") \
    .enableHiveSupport() \
    .getOrCreate()

# 切换到您的数据仓库库
spark.sql("USE flight_dw")
print("INFO: 已切换到数据库: flight_dw")


# --- 2. 加载并准备数据 ---
print("INFO: 开始加载 ml_training_data 表...")
try:
    data = spark.table("ml_training_data")
except Exception as e:
    print(f"ERROR: 加载数据表 'ml_training_data' 失败，请确保第一步Hive脚本已成功执行: {e}")
    spark.stop()
    exit(1)

# 数据清洗与预处理
# 过滤掉无效的测试数据 (基于您之前的脚本)
data = data.filter(F.col("leg_id") != "legid")
# 仅在关键列（尤其是目标变量）为空时才删除行
data = data.na.drop(subset=["total_fare", "departure_airport_fk", "arrival_airport_fk"])
print(f"INFO: 数据加载和初步清洗完成，当前数据行数: {data.count()}")

# --- 3. 定义特征工程流水线 ---
print("INFO: 开始定义特征工程流水线...")

# 【已适配】根据您的表结构，精确定义类别和数值特征列
categorical_cols = [
    "departure_airport_fk",
    "arrival_airport_fk",
    "airline_fk",
    "aircraft_fk",
    "flight_day_of_week",
    "flight_month"
]
numerical_cols = [
    "search_to_flight_days",
    "flight_week_of_year"
]
# 我们要预测的标签列
label_col = "total_fare"

# 创建处理阶段列表
stages = []

# 阶段 A: 使用 Imputer 填充数值列中的空值 (例如用平均值填充)
imputer = Imputer(inputCols=numerical_cols, outputCols=[col + "_imputed" for col in numerical_cols])
stages.append(imputer)

# 阶段 B: 将类别特征转换为数字索引
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep") for col in categorical_cols]
stages.extend(indexers)

# 阶段 C: 对数字索引进行 One-Hot 编码
encoder_inputs = [col + "_index" for col in categorical_cols]
encoder_outputs = [col + "_vec" for col in categorical_cols]
encoder = OneHotEncoder(inputCols=encoder_inputs, outputCols=encoder_outputs)
stages.append(encoder)

# 阶段 D: 将所有处理过的特征合并成一个单一的特征向量
assembler_inputs = [col + "_vec" for col in categorical_cols] + [col + "_imputed" for col in numerical_cols]
vector_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(vector_assembler)

# --- 4. 定义模型并组装总流水线 ---
print("INFO: 定义模型并组装总流水线...")

# 定义梯度提升树回归模型
gbt = GBTRegressor(featuresCol="features", labelCol=label_col)
stages.append(gbt)

# 将所有阶段（特征工程 + 模型）组装成一个完整的 Pipeline
pipeline = Pipeline(stages=stages)


# --- 5. 拆分数据、训练模型 ---
print("INFO: 拆分数据并开始训练模型...")
(training_data, test_data) = data.randomSplit([0.8, 0.2], seed=42)
print(f"INFO: 数据已拆分。训练集行数: {training_data.count()}, 测试集行数: {test_data.count()}")

# 使用训练数据来拟合（训练）整个流水线
model = pipeline.fit(training_data)
print("SUCCESS: 模型训练完成！")


# --- 6. 评估模型性能 ---
print("INFO: 在测试集上评估模型性能...")
predictions = model.transform(test_data)

# 使用 RMSE (均方根误差) 作为评估指标
evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"模型评估结果 - 均方根误差 (RMSE): {rmse}")
print(f"说明: RMSE 代表模型预测票价与真实票价之间的平均差异约为 ${rmse:.2f}。")

# 展示一些预测结果样例
predictions.select(label_col, "prediction").show(10)


# --- 7. 保存训练好的模型 ---
print("INFO: 开始保存模型到 HDFS...")
# 我们将模型保存在一个固定的、易于记忆的路径
model_path = "hdfs:///data/models/flight_price_gbt_model_v1"
model.write().overwrite().save(model_path)
print(f"SUCCESS: 模型已成功保存到: {model_path}")
print("这个路径在后续的预测脚本中将会被用到。")


# --- 8. 关闭 Spark Session ---
spark.stop()
