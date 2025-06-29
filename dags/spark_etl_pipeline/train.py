# train_fare_model.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 导入所有我们需要的ML库
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# ==============================================================================
# 1. 初始化 Spark Session
# ==============================================================================
spark = SparkSession.builder \
    .appName("Train_Fare_Prediction_Model") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

print("INFO: Spark Session 初始化成功！")
db_name = "flight_dw"
spark.sql(f"USE {db_name}")

# ==============================================================================
# 2. 加载数据并准备
# ==============================================================================
print(f"INFO: 正在从 `{db_name}.ml_training_data` 加载数据...")
all_data = spark.table("ml_training_data")


# ==============================================================================
# 2.5 【核心修复】增加空值填充步骤
# ==============================================================================
print("INFO: 正在对特征列进行空值填充，以增强模型稳定性...")

# 定义需要检查的特征列
numerical_cols = ["search_to_flight_days", "flight_week_of_year"]
categorical_cols = [
    "departure_airport_fk", "arrival_airport_fk", "airline_fk",
    "aircraft_fk", "flight_day_of_week", "flight_month"
]

# 使用 .na.fill() 方法填充空值
# 对数值类型的列，我们用 0 来填充
# 对分类型（外键）的列，我们也用 0 来填充，因为0通常不代表任何一个有效的外键ID
imputed_data = all_data.na.fill(0, subset=numerical_cols + categorical_cols)

print("SUCCESS: 空值填充完成！")













# 将数据随机划分为训练集（80%）和测试集（20%）
# 我们用训练集来“教”模型，用测试集来“考”模型，看它学得好不好
train_data, test_data = all_data.randomSplit([0.8, 0.2], seed=42)
print("INFO: 数据已划分为训练集和测试集。")
print(f"训练集行数: {train_data.count()}, 测试集行数: {test_data.count()}")


# ==============================================================================
# 3. 定义机器学习流水线 (Pipeline)
# ==============================================================================
print("INFO: 正在定义机器学习流水线...")





# --- a. 特征转换：处理分类特征 ---
# 定义哪些列是分类特征
categorical_cols = [
    "departure_airport_fk", "arrival_airport_fk", "airline_fk",
    "aircraft_fk", "flight_day_of_week", "flight_month"
]

# 为每个分类特征创建一个StringIndexer。它会把每个类别映射到一个数字索引
# 例如：['PVG', 'JFK', 'PVG'] -> [0.0, 1.0, 0.0]
string_indexers = [
    StringIndexer(
        inputCol=col,
        outputCol=col + "_idx",
        handleInvalid="keep"  # <-- 核心修改点！告诉它不要报错，而是将无效值归为一个特殊类别
    )
    for col in categorical_cols
]

# --- b. 特征向量化：合并所有特征 ---
# 定义哪些列是数值特征
numerical_cols = ["search_to_flight_days", "flight_week_of_year"]

# 将处理后的分类特征列和原始的数值特征列的名称集合在一起
assembler_inputs = [col + "_idx" for col in categorical_cols] + numerical_cols

# 创建一个VectorAssembler，它会把所有输入列合并成一个名为 "features" 的向量列
vector_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")


# --- c. 定义模型算法 ---
# 创建一个梯度提升树回归器
# labelCol 告诉模型我们要预测的是哪一列 (`total_fare`)
# featuresCol 告诉模型从哪里获取特征 (`features`向量列)
gbt_regressor = GBTRegressor(featuresCol="features", labelCol="total_fare", maxIter=10)


# --- d. 组装完整的流水线 ---
# 按顺序将所有步骤放入Pipeline中
pipeline = Pipeline(stages=string_indexers + [vector_assembler, gbt_regressor])


# ==============================================================================
# 4. 训练模型
# ==============================================================================
print("INFO: 流水线已定义，开始在训练集上训练模型... 这可能需要几分钟...")

# 调用 .fit() 方法，Spark将自动执行流水线中的所有步骤，并训练模型
fare_prediction_model = pipeline.fit(train_data)

print("SUCCESS: 票价预测模型训练完成！")


# ==============================================================================
# 5. 评估模型性能
# ==============================================================================
print("INFO: 正在测试集上评估模型性能...")

# 使用训练好的模型对“看不见”的测试数据进行预测
predictions = fare_prediction_model.transform(test_data)

# 查看几条预测结果
predictions.select("total_fare", "prediction").show(5)

# 使用回归评估器来计算模型的误差
# 我们使用 RMSE (均方根误差) 作为评估指标
evaluator = RegressionEvaluator(
    labelCol="total_fare",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)

print("--------------------------------------------------")
print(f"SUCCESS: 模型评估完成！")
print(f"均方根误差 (RMSE) on Test Data = {rmse:.2f}")
print(f"解读: 这代表我们的模型预测的票价，平均来说与真实票价相差约 {rmse:.2f} 元。这个值越低，说明模型越准。")
print("--------------------------------------------------")


# ==============================================================================
# 6. 保存训练好的模型
# ==============================================================================
MODEL_PATH = "hdfs:///models/fare_prediction_model"
print(f"INFO: 正在将训练好的模型保存到 HDFS 路径: {MODEL_PATH}")

# 我们保存的是整个训练好的Pipeline，包含了所有的特征处理步骤
# 这样在未来使用时，我们只需要把原始数据喂给它，它就能自动处理并预测
fare_prediction_model.write().overwrite().save(MODEL_PATH)

print(f"SUCCESS: 模型已成功保存！")


# ==============================================================================
# 7. 结束任务
# ==============================================================================
spark.stop()