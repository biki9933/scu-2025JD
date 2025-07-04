import os
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnableLambda
from langchain_core.runnables import RunnablePassthrough

# --- 1. 配置基础环境 ---
def clean_sql_query(query: str) -> str:
    """一个简单的函数，用于清理LLM可能生成的多余前缀。"""
    # 寻找"SELECT"关键字（不区分大小写）
    select_pos = query.upper().find("SELECT")
    # 如果找到了，就从"SELECT"开始截取字符串，并去掉首尾多余的空格
    if select_pos != -1:
        return query[select_pos:].strip()
    # 如果没找到，返回原字符串（以防万一）
    return query.strip()
# 设置您的OpenAI API密钥
# 强烈建议使用环境变量，而不是直接写在代码里
# OPENAI_API_KEY = 'sk-a1a52b62f26a436ea2b209045a88fa71'
# 如果您是临时测试，也可以取消下面这行的注释，并填入您的Key
#os.environ["OPENAI_API_KEY"] = "sk-a1a52b62f26a436ea2b209045a88fa71"
deepseek_api_key = "sk-a1a52b62f26a436ea2b209045a88fa71"  # 替换为您的DeepSeek API Key
deepseek_base_url = "https://api.deepseek.com" # 这是DeepSeek的官方API地址

# 配置您的MySQL数据库连接信息
# 格式: "mysql+mysqlconnector://<用户名>:<密码>@<主机地址>:<端口>/<数据库名>"
db_user = "root"
db_password = "qweasd321"
db_host = "192.168.101.51"  # 如果MySQL和这个脚本在同一台虚拟机上，就是localhost
db_port = "3306"       # MySQL默认端口
db_name = "flight_dw"  # 您之前创建的数据库名

# 创建SQLAlchemy数据库连接URI
db_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# --- 2. 初始化LangChain组件 ---

# 初始化大语言模型 (LLM)
# 我们使用OpenAI的gpt-3.5-turbo模型，temperature=0表示让它尽可能给出稳定、确切的回答
llm = ChatOpenAI(
    model="deepseek-chat",              # 指定使用DeepSeek的模型名称
    temperature=0,
    api_key=deepseek_api_key,           # 传入您的DeepSeek API Key
    base_url=deepseek_base_url          # 传入DeepSeek的Base URL
)

# 连接到您的数据库
# LangChain通过这个db对象来了解您的数据库结构
db = SQLDatabase.from_uri(db_uri)

# --- 3. 创建执行链 (Chain) ---

# 创建一个专门用于执行SQL查询的工具
execute_query_tool = QuerySQLDataBaseTool(db=db)

# 创建一个将自然语言转换为SQL查询的链
write_query_chain = create_sql_query_chain(llm, db)

# 创建一个最终的执行链 (chain)
# 这是一个非常强大的模式，它将整个流程串联起来：
# 1. RunnablePassthrough: 将用户的问题传递下去。
# 2. write_query_chain: 接收问题，生成SQL查询。
# 3. execute_query_tool: 接收SQL查询，执行它，并返回结果。
# 修改后的链
chain = (
    RunnablePassthrough.assign(query=write_query_chain).assign(
        # 修改后的链
        result=itemgetter("query") | RunnableLambda(clean_sql_query) | execute_query_tool
    )
)
# --- 4. 运行并提问 ---

# 提出您的问题
question = "dim_airport中，iata_code= CSE的对应的城市是？输出中文回复"

# 调用链来执行
response = chain.invoke({"question": question})

# --- 5. 查看结果 ---

print("--- 用户问题 ---")
print(response["question"])
print("\n--- AI生成的SQL查询 ---")
print(response["query"])
print("\n--- SQL查询结果 ---")
print(response["result"])

# 我们可以进一步让AI将最终结果也用自然语言总结一下
prompt_template = """
根据以下用户问题、SQL查询和查询结果，生成一个最终的、通顺的自然语言回答。

问题: {question}
SQL查询: {query}
SQL结果: {result}

回答:
"""

prompt = PromptTemplate.from_template(prompt_template)

# 创建一个完整的链，从提问到最终自然语言回答
full_chain = chain | prompt | llm | StrOutputParser()

final_answer = full_chain.invoke({"question": question})

print("\n--- AI的最终回答 ---")
print(final_answer)