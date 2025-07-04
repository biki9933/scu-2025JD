# prepare_knowledge.py
import mysql.connector

# --- 您的数据库连接信息 ---
config = {
    'user': 'your_mysql_user',
    'password': 'your_mysql_password',
    'host': 'localhost',
    'database': 'flight_dw',
    'raise_on_warnings': True
}
output_filename = "knowledge_base.txt"

try:
    # 连接数据库
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    print("数据库连接成功！")

    with open(output_filename, "w", encoding="utf-8") as f:
        # 1. 从 dim_airport 表提取知识
        print("正在从 dim_airport 表提取知识...")
        cursor.execute("SELECT airport_name, city, country_code, iata_code FROM dim_airport WHERE airport_name IS NOT NULL AND city IS NOT NULL;")
        airports = cursor.fetchall()
        for name, city, country, iata in airports:
            if name and city and iata:
                f.write(f"机场代码为{iata}的机场是{name}，它位于{country}的{city}市。\n")

        # 2. 从 dim_airline 表提取知识
        print("正在从 dim_airline 表提取知识...")
        cursor.execute("SELECT airline_name, airline_code FROM dim_airline WHERE airline_name IS NOT NULL;")
        airlines = cursor.fetchall()
        for name, code in airlines:
            if name and code:
                f.write(f"航空公司的名称是{name}，其二字代码是{code}。\n")

        # 您可以按照这个模式，继续从其他表中提取更多知识...

    print(f"知识库文件 '{output_filename}' 已成功生成！")

except mysql.connector.Error as err:
    print(f"数据库错误: {err}")
finally:
    if 'cnx' in locals() and cnx.is_connected():
        cursor.close()
        cnx.close()
        print("数据库连接已关闭。")