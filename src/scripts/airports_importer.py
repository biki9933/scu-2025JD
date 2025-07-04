import pandas as pd
import pymysql
from sqlalchemy import create_engine, VARCHAR
from sqlalchemy.dialects.mysql import LONGTEXT
import chardet

# 数据库连接配置
db_config = {
    'host': '192.168.101.49',
    'port': 3306,
    'user': 'hive',
    'password': '123456',
    'database': 'raw_data'
}


def detect_file_encoding(file_path):
    with open(file_path, 'rb') as f:
        rawdata = f.read(100000)
    return chardet.detect(rawdata)['encoding']


def read_csv_with_encoding(file_path, usecols=None):
    encodings = ['utf-8', 'latin1', 'iso-8859-1', 'gbk', 'cp1252']

    # 首先尝试自动检测编码
    try:
        detected_encoding = detect_file_encoding(file_path)
        if detected_encoding not in encodings:
            encodings.insert(0, detected_encoding)
    except:
        pass

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding, usecols=usecols)
            print(f"成功使用 {encoding} 编码读取文件")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"尝试 {encoding} 编码时出错: {e}")
            continue

    try:
        df = pd.read_csv(file_path, encoding='utf-8', errors='ignore', usecols=usecols)
        print("使用UTF-8编码并忽略错误字符读取文件")
        return df
    except Exception as e:
        print(f"最终读取文件失败: {e}")
        return None


def process_airports(file_path):
    cols_to_use = ['type', 'name', 'latitude_deg', 'longitude_deg', 'iso_country', 'iso_region', 'municipality']
    df = read_csv_with_encoding(file_path, usecols=cols_to_use)

    if df is not None:
        df = df.dropna(how='all')
        df = df.fillna('')
        df['latitude_deg'] = pd.to_numeric(df['latitude_deg'], errors='coerce')
        df['longitude_deg'] = pd.to_numeric(df['longitude_deg'], errors='coerce')

    return df


def import_to_database(df, table_name):
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

        if df is not None:
            df.to_sql(
                name='airports',
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000,
                dtype={
                    'name': VARCHAR(255, collation='utf8mb4_unicode_ci'),
                    'type': VARCHAR(255, collation='utf8mb4_unicode_ci'),
                    # 其他文本字段...
                }
            )
            print(f"成功导入{table_name}数据")
            return True
        else:
            print("没有有效数据可导入")
            return False
    except Exception as e:
        print(f"导入数据库失败: {e}")
        return False


def main():
    airports_file = '../airports.csv'

    print("正在处理airports.csv...")
    airports_df = process_airports(airports_file)

    if airports_df is not None:
        print("\n正在导入数据到数据库...")
        success = import_to_database(airports_df, 'airports')
        if success:
            print("\nairports数据导入完成！")
    else:
        print("\n没有有效的airports数据可导入")


if __name__ == '__main__':
    main()