import asyncio
import os
import time

from multiprocessing import Process
from graphrag_api.search import SearchRunner
from db_utils.search import search_vectors
from flask import Flask, request, jsonify
import logging
from graphrag_api.index import GraphRagIndexer
import schedule

from neo4j_db.insert_db import neo4j_db

app = Flask(__name__)

# 配置日志
log_file = "./logging.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def run_indexer():
    create_dir()

    try:
        indexer = GraphRagIndexer(root="rag")
        asyncio.run(indexer.run())

    except Exception as e:
        logging.error("Error in indexer run: %s", e)


@app.route('/graphrag/chat', methods=['POST'])
def graphrag_chat():
    search_runner = SearchRunner(root_dir="rag")
    data = request.get_json()
    if 'chat_type' in data and 'content' in data:
        chat_type = data['chat_type']
        content = data['content']
        remove_type = data.get('remove_type',False)
        # 根据 chat_type 的值调用相应的函数
        if remove_type:
            if chat_type == 'local_search':
                response = search_runner.remove_sources(search_runner.run_local_search(query=content, streaming=False))
            elif chat_type == 'global_search':
                response = search_runner.remove_sources(search_runner.run_global_search(query=content, streaming=False))
            else:
                # 如果 chat_type 不是有效值，返回错误信息
                response = {"error": f"无效的 chat_type: {chat_type}"}
        else:
            if chat_type == 'local_search':
                response = search_runner.run_local_search(query=content, streaming=False)
            elif chat_type == 'global_search':
                response = search_runner.run_global_search(query=content, streaming=False)
            else:
                # 如果 chat_type 不是有效值，返回错误信息
                response = {"error": f"无效的 chat_type: {chat_type}"}
    else:
        # 如果请求体中缺少 chat_type 或 content，返回错误信息
        response = {"error": "请求体中必须包含 chat_type 和 content 字段"}

    return jsonify(response)


def create_dir():
    # 定义 rag 目录路径
    rag_directory = "./rag"

    # 创建 input 文件夹路径
    input_folder = os.path.join(rag_directory, "input")

    # 确保 input 文件夹存在
    os.makedirs(input_folder, exist_ok=True)

    # 创建 input.txt 文件路径
    input_file_path = os.path.join(input_folder, "input.txt")

    # 写入一些内容到 input.txt 文件
    try:
        with open(input_file_path, "w", encoding="utf-8") as input_file:
            list_info = search_vectors()
            for data in list_info:
                result = (
                    f"名称: {data['cmetadata']['name']}\n"
                    f"描述: {data['cmetadata']['description']}\n"
                    f"文档链接: {data['cmetadata']['document_url']}\n"
                    f"项目类型: {data['cmetadata']['project_type']}\n"
                    f"相关人员: {data['cmetadata']['related_person']}\n"
                    f"技术栈: {data['cmetadata']['technology_stack']}\n"
                    f"其他说明: {data['cmetadata']['other_explanation']}\n"
                    f"ID: {data['id']}.\n\n"
                )
                input_file.write(result)
                # logging.info(f"Folder and file created at: {input_file_path}")
    except Exception as e:
        logging.info(e)
        return False

    return True


def start_flask():
    app.run(host='0.0.0.0', port=6240, use_reloader=False)  # 禁用重载


# if __name__ == '__main__':
#     # thread = threading.Thread(target=start_flask, daemon=True)
#     # thread.start()
#     # thread.join()
#     app.run(host="0.0.0.0", port=6240, use_reloader=False)
def schedule_indexer():
    # schedule.every(7).days.do(run_indexer)
    schedule.every(168).hours.do(run_indexer)
    while True:
        schedule.run_pending()
        time.sleep(1)


def schedule_insert_db():
    neo4j_handle = neo4j_db()
    schedule.every(169).hours.do(neo4j_handle.insert_db)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':

    # 在一个新的进程中运行调度器
    scheduler_process = Process(target=schedule_indexer)
    scheduler_process.start()

    # 创建并启动 Neo4j 插入的进程
    insert_db_process = Process(target=schedule_insert_db)
    insert_db_process.start()

    # 在主线程中启动 Flask
    start_flask()

    scheduler_process.join()
    insert_db_process.join()
