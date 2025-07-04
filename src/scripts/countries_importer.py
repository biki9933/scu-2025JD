import pandas as pd
import pymysql
from sqlalchemy import create_engine
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


def process_countries(file_path):
    df = read_csv_with_encoding(file_path)

    if df is not None:
        df = df.dropna(how='all')
        df.columns = ['code', 'name']

    return df


def import_to_database(df, table_name):
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

        if df is not None:
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
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
    countries_file = '../countries.csv'

    print("正在处理countries.csv...")
    countries_df = process_countries(countries_file)

    if countries_df is not None:
        print("\n正在导入数据到数据库...")
        success = import_to_database(countries_df, 'countries')
        if success:
            print("\ncountries数据导入完成！")
    else:
        print("\n没有有效的countries数据可导入")


if __name__ == '__main__':
    main()