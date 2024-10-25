import os
import shutil

def find_newest_output_dir(base_path):
    # 获取output文件夹路径
    output_path = os.path.join(base_path, 'output')
    print(output_path)
    # 初始化最新时间戳和最新目录为None
    newest_timestamp = None
    newest_dir = None

    # 遍历output文件夹下的所有子目录
    for subdir in os.listdir(output_path):
        if os.path.isdir(os.path.join(output_path, subdir)):
            # 提取子目录名中的时间戳部分
            try:
                timestamp = int(subdir.split('-')[0]) + int(subdir.split('-')[1])
            except ValueError:
                continue  # 如果无法提取时间戳，则跳过该子目录

            # 比较当前时间戳与已知最新时间戳
            if newest_timestamp is None or timestamp > newest_timestamp:
                newest_timestamp = timestamp
                newest_dir = subdir

    for subdir in os.listdir(output_path):
        tem_path = os.path.join(output_path, subdir)
        if os.path.isdir(tem_path) and subdir != newest_dir:
            shutil.rmtree(tem_path)
            print(f"Deleted folder: {tem_path}")
    return os.path.join(output_path, newest_dir)

# find_newest_output_dir("./rag")


# 假设你的项目根目录是D:\code\grappgrag_flask
project_root = r'./rag'
newest_output_dir = find_newest_output_dir(project_root)
print(f"The newest output directory is: {newest_output_dir}")

# import os
# import asyncio
# import time
#
# from graphrag_api.search import SearchRunner
# from flask_socketio import SocketIO
# from db_utils.search import search_vectors
# from flask import Flask, request, jsonify
# import logging
# from graphrag_api.index import GraphRagIndexer
# import config
# import threading
# import threading
# import asyncio
# import logging
# from flask import Flask
#
# app = Flask(__name__)
#
# log_file = "./logging.log"
# logging.basicConfig(filename=log_file, level=logging.INFO,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# def run_indexer():
#     content = ""
#
#     loop = asyncio.new_event_loop()  # 创建一个新的事件循环
#     asyncio.set_event_loop(loop)  # 设置为当前事件循环
#
#     indexer = GraphRagIndexer(root="rag", init=True)
#     try:
#         loop.run_until_complete(asyncio.wait_for(indexer.run(), timeout=10))
#     except asyncio.TimeoutError:
#         logging.error("索引器运行超时")
#     content += "GraphRag 初始化成功,"
#     print(content)
#     create_dir_result = "create_dir()"
#     if create_dir_result:
#         content += "create_dir成功,"
#     else:
#         content += "create_dir失败,"
#         logging.info("create_dir失败")
#
#     write_settings_result = "write_settings()"
#     write_env_result = "write_env()"
#
#     if write_settings_result and write_env_result:
#         content += "write_settings and write_env_result成功,"
#     else:
#         content += "write_settings and write_env_result失败,"
#         logging.info("write_settings and write_env_result失败")
#
#     indexer = GraphRagIndexer(root="rag")
#     loop.run_until_complete(indexer.run())  # 再次运行异步方法
#     content += "再次初始化成功"
#
#     print(content.strip(","))
#
#
# def start_flask():
#     app.run(host='0.0.0.0', port=5000)  # Flask 服务器的设置
#
#
# if __name__ == '__main__':
#     # 启动索引器的线程
#     indexer_thread = threading.Thread(target=run_indexer, daemon=True)
#     indexer_thread.start()
#
#     # 启动 Flask 服务器
#     start_flask()
