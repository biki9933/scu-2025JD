# clean_itinerary_path_final_fixed.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import traceback


def clean_path_string(path_string):
    if path_string is None:
        return None
    separator = " -> "
    path_array = [node.strip() for node in path_string.split(separator)]
    if not path_array: return ""
    cleaned_path = [path_array[0]]
    for i in range(1, len(path_array)):
        if path_array[i].strip() != cleaned_path[-1].strip():
            cleaned_path.append(path_array[i])
    return separator.join(cleaned_path)

clean_path_udf = F.udf(clean_path_string, StringType())


def main():
    """
    【数据清洗脚本】
    """
    # --- 1. 初始化 Spark Session ---
    spark = SparkSession.builder \
        .appName("Clean Itinerary Details Path (Fixed)") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()
        
    # --- 2. 配置 ---
    source_table_name = "flight_dw.itinerary_details"
    target_table_name = "flight_dw.itinerary_details_cleaned"
    
    print(f"--- 开始清洗表 '{source_table_name}' ---")
    
    source_df = None
    try:
        # --- 3. 读取源表 ---
        print(f"INFO: 读取源表: {source_table_name}")
        source_df = spark.table(source_table_name)
        source_df.cache()
        
        # --- 4. 应用UDF清洗 full_path 列 ---
        print("\n[步骤1] 应用清洗 full_path 列...")
        
        # 确保DataFrame中包含了分区键列，以便insertInto能找到它们
        # spark.table()读取分区表时会自动包含分区键列，所以这里没问题。
        cleaned_df = source_df.withColumn(
            "full_path", 
            clean_path_udf(F.col("full_path"))
        )
        
        # --- 5. 【核心修正】将修正后的数据写入新表 ---
        print(f"\n写入新表: {target_table_name}")
        
        # 直接使用 insertInto，去掉 partitionBy
        cleaned_df.write \
            .mode("overwrite") \
            .insertInto(target_table_name) # <<< 直接调用 insertInto

        print(f"\n--- 成功！数据清洗完成。---")
        print(f"修正后的数据已保存到新表: {target_table_name}")

    except Exception as e:
        print(f"\n错误: 在清洗过程中发生异常!")
        traceback.print_exc()
    
    finally:
        if source_df is not None and source_df.is_cached:
            source_df.unpersist()
        print("INFO: 正在关闭 Spark Session...")
        spark.stop()

if __name__ == '__main__':
    main()
