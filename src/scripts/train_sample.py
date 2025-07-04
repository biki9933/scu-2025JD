# =================================================================
# 文件名: train_lightweight_model.py
# 描述: (轻量级方案) 通过采样减少训练数据量，快速训练并保存模型
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
    .appName("LightweightFlightTraining") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("USE flight_dw")
print("INFO: 已切换到数据库: flight_dw")

# --- 2. 加载数据并进行采样 ---
print("INFO: 开始加载全量 ml_training_data 表...")
try:
    full_data = spark.table("ml_training_data")
except Exception as e:
    print(f"ERROR: 加载数据表 'ml_training_data' 失败: {e}")
    spark.stop()
    exit(1)

# 【核心优化】从此开始，我们不再使用全部数据
# withReplacement=False 表示不放回抽样
# fraction=0.1 表示我们随机抽取10%的数据作为样本
# seed=42 保证每次运行抽样的结果都一样，便于复现
sample_fraction = 0.01
data = full_data.sample(withReplacement=False, fraction=sample_fraction, seed=42)
print(f"INFO: 全量数据行数: {full_data.count()}, 已采样 {sample_fraction*100}% 的数据用于训练, 样本行数: {data.count()}")

# 缓存样本数据，可以提高后续计算效率
data.cache()

# --- 3. 定义特征工程流水线 (与之前完全相同) ---
# ... (这部分代码与上一版脚本完全一致，此处省略以保持简洁)
# ... 您只需复制上一版脚本中第3、4、5、6、7、8节的代码即可 ...
# 下面我将这部分代码也完整列出，方便您直接复制

print("INFO: 开始定义特征工程流水线...")
categorical_cols = ["departure_airport_fk", "arrival_airport_fk", "airline_fk", "aircraft_fk", "flight_day_of_week", "flight_month"]
numerical_cols = ["search_to_flight_days", "flight_week_of_year"]
label_col = "total_fare"
stages = []
imputer = Imputer(inputCols=numerical_cols, outputCols=[col + "_imputed" for col in numerical_cols])
stages.append(imputer)
indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep") for col in categorical_cols]
stages.extend(indexers)
encoder_inputs = [col + "_index" for col in categorical_cols]
encoder_outputs = [col + "_vec" for col in categorical_cols]
encoder = OneHotEncoder(inputCols=encoder_inputs, outputCols=encoder_outputs)
stages.append(encoder)
assembler_inputs = [col + "_vec" for col in categorical_cols] + [col + "_imputed" for col in numerical_cols]
vector_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
stages.append(vector_assembler)

# --- 4. 定义模型并组装总流水线 ---
print("INFO: 定义模型并组装总流水线...")
gbt = GBTRegressor(featuresCol="features", labelCol=label_col)
stages.append(gbt)
pipeline = Pipeline(stages=stages)

# --- 5. 拆分数据、训练模型 ---
print("INFO: 拆分采样的后数据并开始训练模型...")
(training_data, test_data) = data.randomSplit([0.8, 0.2], seed=42)
print(f"INFO: 数据已拆分。训练集行数: {training_data.count()}, 测试集行数: {test_data.count()}")
model = pipeline.fit(training_data)
print("SUCCESS: 模型训练完成！")

# --- 6. 评估模型性能 ---
print("INFO: 在测试集上评估模型性能...")
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"模型评估结果 - 均方根误差 (RMSE): {rmse}")
predictions.select(label_col, "prediction").show(10)

# --- 7. 保存训练好的模型 ---
print("INFO: 开始保存轻量级模型到 HDFS...")
model_path = "hdfs:///data/models/flight_price_gbt_model_lightweight_v1"
model.write().overwrite().save(model_path)
print(f"SUCCESS: 轻量级模型已成功保存到: {model_path}")

# --- 8. 关闭 Spark Session ---
spark.stop()
