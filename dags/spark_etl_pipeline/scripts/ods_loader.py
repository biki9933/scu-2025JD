import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ods_loader.py <input_path> <ods_table_name>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]  # e.g., 'hdfs:///user/airflow/raw_data/sales.csv'
    ods_table_name = sys.argv[2]  # e.g., 'ods.sales_raw'

    # 1. 创建支持 Hive 的 SparkSession
    spark = SparkSession.builder \
        .appName(f"ODS Load for {ods_table_name}") \
        .enableHiveSupport() \
        .getOrCreate()

    print(f"Spark Session created for ODS loading.")

    # 2. 从 HDFS 读取原始数据 (直接读取原始json数据，不解析)
    try:
        # text读取的数据的列为value，将其重命名为json_body
        raw_df = (spark.read.text(input_path)
                  .withColumnRenamed('value', 'json_body'))
        print(f"Successfully read data from HDFS path: {input_path}")
        raw_df.printSchema()

        # 3. 创建 Hive 数据库（如果不存在）
        db_name = ods_table_name.split('.')[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' ensured to exist.")

        # 6. 创建 Hive 表（如果不存在）
        create_table_sql = f"""
                        CREATE TABLE IF NOT EXISTS {ods_table_name} (
                            json_body string
                        )
                        """
        spark.sql(create_table_sql)
        print(f"Table {ods_table_name} with partitioning ensured to exist.")

        # 4. 将 DataFrame 写入 Hive ODS 表（覆盖模式）
        # 使用 saveAsTable 会将数据以 Parquet 格式存储，并注册到 Hive Metastore
        raw_df.write.mode("overwrite").saveAsTable(ods_table_name)
        print(f"Successfully loaded data into ODS table: {ods_table_name}")

    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        spark.stop()
        sys.exit(1)

    # 5. 停止 SparkSession
    spark.stop()
    print("ODS loading process finished successfully.")
