import streamlit as st
import os
from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from operator import itemgetter
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough, RunnableLambda


# --- 核心函数封装 ---

# 使用Streamlit的缓存功能，避免每次交互都重新连接数据库和初始化模型
# 这能极大提升网页应用的响应速度
@st.cache_resource
def get_chain():
    """
    这个函数负责所有昂贵的初始化操作，并返回可执行的LangChain链。
    """
    # 1. 配置环境
    deepseek_api_key = "sk-a1a52b62f26a436ea2b209045a88fa71"
    deepseek_base_url = "https://api.deepseek.com"  # 这是DeepSeek的官方API地址
    db_user = "root"
    db_password = "qweasd321"
    db_host = "192.168.101.51"  # 如果MySQL和这个脚本在同一台虚拟机上，就是localhost
    db_port = "3306"  # MySQL默认端口
    db_name = "flight_dw"  # 您之前创建的数据库名
    db_uri = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"


    # 2. 初始化组件
    llm = ChatOpenAI(model="deepseek-chat", temperature=0, api_key=deepseek_api_key, base_url=deepseek_base_url)
    db = SQLDatabase.from_uri(db_uri)
    execute_query_tool = QuerySQLDataBaseTool(db=db)
    write_query_chain = create_sql_query_chain(llm, db)

    # 3. 定义清洗函数和最终的链
    def clean_sql_query(query: str) -> str:
        select_pos = query.upper().find("SELECT")
        if select_pos != -1:
            return query[select_pos:].strip()
        return query.strip()

    answer_prompt = PromptTemplate.from_template(
        """
        根据以下用户问题、SQL查询和查询结果，生成一个最终的、通顺的自然语言回答。

        问题: {question}
        SQL查询: {query}
        SQL结果: {result}

        回答:
        """
    )

    rephrased_chain = (
        RunnablePassthrough.assign(query=write_query_chain).assign(
            result=itemgetter("query") | RunnableLambda(clean_sql_query) | execute_query_tool
        )
    )

    full_chain = rephrased_chain | answer_prompt | llm | StrOutputParser()

    # 我们返回两个链，一个用于最终回答，一个用于中间步骤展示
    return full_chain, rephrased_chain


# --- Streamlit 界面构建 ---

st.title("✈️ 智能航班数据助手")

# 获取处理链
final_chain, intermediate_chain = get_chain()

# 创建一个表单，让输入框和按钮在一起
with st.form("input_form"):
    # 使用文本输入框让用户提问
    question = st.text_input("您好！请输入您关于航班数据的问题：", value="哪家航空公司的拼接票比例最高？")
    # 创建一个提交按钮
    submit_button = st.form_submit_button(label="提问")

# 如果用户点击了按钮
if submit_button:
    if not question:
        st.warning("请输入您的问题！")
    else:
        # 显示一个加载提示
        with st.spinner("AI助手正在思考并查询数据库，请稍候..."):
            try:
                # 首先获取中间结果，用于展示
                intermediate_response = intermediate_chain.invoke({"question": question})

                # 然后获取最终的自然语言回答
                final_answer = final_chain.invoke({"question": question})

                st.subheader("📝 AI的回答")
                st.markdown(final_answer)

                # 使用展开组件来展示AI的“思考过程”
                with st.expander("查看AI的思考过程..."):
                    st.markdown("##### 1. 自然语言问题：")
                    st.info(intermediate_response['question'])
                    st.markdown("##### 2. 生成的SQL查询：")
                    st.code(intermediate_response['query'], language="sql")
                    st.markdown("##### 3. SQL查询结果：")
                    st.code(str(intermediate_response['result']))

            except Exception as e:
                st.error(f"处理过程中出现错误：{e}")