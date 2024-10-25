import logging
import os
import shutil

import pandas as pd
from neo4j import GraphDatabase
import time
import config



# 配置日志
log_file = "./logging.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
class neo4j_db():
    def __init__(self):
        self.NEO4J_URL = config.NEO4J_URL
        self.NEO4J_USERNAME = config.NEO4J_USERNAME
        self.NEO4J_PASSWORD = config.NEO4J_PASSWORD
        self.NEO4J_DATABASE = config.NEO4J_DATABASE

        self.driver = GraphDatabase.driver(self.NEO4J_URL, auth=(self.NEO4J_USERNAME, self.NEO4J_PASSWORD))

    # 清空数据库
    def clear_database(self,):
        with self.driver.session(database=self.NEO4J_DATABASE) as session:
            session.run("MATCH (n) DETACH DELETE n")  # 删除所有节点及其关系


    def batched_import(self,statement, df, batch_size=1000):
        total = len(df)
        start_s = time.time()
        for start in range(0,total, batch_size):
            batch = df.iloc[start: min(start+batch_size,total)]
            result = self.driver.execute_query("UNWIND $rows AS value " + statement,
                                          rows=batch.to_dict('records'),
                                          database_=self.NEO4J_DATABASE)
            print(result.summary.counters)
        print(f'{total} rows in { time.time() - start_s} s.')
        return total

    def create_statements(self):
        statements = """  
        create constraint chunk_id if not exists for (c:__Chunk__) require c.id is unique;  
        create constraint document_id if not exists for (d:__Document__) require d.id is unique;  
        create constraint entity_id if not exists for (c:__Community__) require c.community is unique;  
        create constraint entity_id if not exists for (e:__Entity__) require e.id is unique;  
        create constraint entity_title if not exists for (e:__Entity__) require e.name is unique;  
        create constraint entity_title if not exists for (e:__Covariate__) require e.title is unique;  
        create constraint related_id if not exists for ()-[rel:RELATED]->() require rel.id is unique;  
        """.split(";")
        for statement in statements:
            if len((statement or "").strip()) > 0:
                print(statement)
                self.driver.execute_query(statement)

    def insert_db(self):
        self.clear_database()
        self.create_statements()
        # 获取最新init的文件夹
        newest_dir = self.find_newest_output_dir()

        GRAPHRAG_FOLDER = os.path.join(newest_dir, "artifacts")
        # documents节点
        doc_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_documents.parquet', columns=["id", "title"])
        doc_df.head(2)
        # import documents
        statement = """  
        MERGE (d:__Document__ {id:value.id})  
        SET d += value {.title}  
        """
        self.batched_import(statement, doc_df)

        # text_units节点
        text_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_text_units.parquet',
                                  columns=["id", "text", "n_tokens", "document_ids"])
        statement = """  
        MERGE (c:__Chunk__ {id:value.id})  
        SET c += value {.text, .n_tokens}  
        WITH c, value  
        UNWIND value.document_ids AS document  
        MATCH (d:__Document__ {id:document})  
        MERGE (c)-[:PART_OF]->(d)  
        """
        self.batched_import(statement, text_df)

        #entities
        entity_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_entities.parquet',
                                    columns=["name", "type", "description", "human_readable_id", "id",
                                             "description_embedding", "text_unit_ids"])
        entity_statement = """  
        MERGE (e:__Entity__ {id:value.id})  
        SET e += value {.human_readable_id, .description, name:replace(value.name,'"','')}  
        WITH e, value  
        CALL db.create.setNodeVectorProperty(e, "description_embedding", value.description_embedding)  
        CALL apoc.create.addLabels(e, case when coalesce(value.type,"") = "" then [] else [apoc.text.upperCamelCase(replace(value.type,'"',''))] end) yield node  
        UNWIND value.text_unit_ids AS text_unit  
        MATCH (c:__Chunk__ {id:text_unit})  
        MERGE (c)-[:HAS_ENTITY]->(e)  
        """
        self.batched_import(entity_statement, entity_df)

        #relationships
        rel_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_relationships.parquet',
                                 columns=["source", "target", "id", "rank", "weight", "human_readable_id", "description",
                                          "text_unit_ids"])
        rel_statement = """  
            MATCH (source:__Entity__ {name:replace(value.source,'"','')})  
            MATCH (target:__Entity__ {name:replace(value.target,'"','')})  
            // not necessary to merge on id as there is only one relationship per pair  
            MERGE (source)-[rel:RELATED {id: value.id}]->(target)  
            SET rel += value {.rank, .weight, .human_readable_id, .description, .text_unit_ids}  
            RETURN count(*) as createdRels  
        """
        self.batched_import(rel_statement, rel_df)


        #communities
        community_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_communities.parquet',
                                       columns=["id", "level", "title", "text_unit_ids", "relationship_ids"])
        statement = """  
        MERGE (c:__Community__ {community:value.id})  
        SET c += value {.level, .title}  
        /*  
        UNWIND value.text_unit_ids as text_unit_id  
        MATCH (t:__Chunk__ {id:text_unit_id})  
        MERGE (c)-[:HAS_CHUNK]->(t)  
        WITH distinct c, value  
        */  
        WITH *  
        UNWIND value.relationship_ids as rel_id  
        MATCH (start:__Entity__)-[:RELATED {id:rel_id}]->(end:__Entity__)  
        MERGE (start)-[:IN_COMMUNITY]->(c)  
        MERGE (end)-[:IN_COMMUNITY]->(c)  
        RETURN count(distinct c) as createdCommunities  
        """
        self.batched_import(statement, community_df)

        #community_reports
        community_report_df = pd.read_parquet(f'{GRAPHRAG_FOLDER}/create_final_community_reports.parquet',
                                              columns=["id", "community", "level", "title", "summary", "findings",
                                                       "rank", "rank_explanation", "full_content"])
        community_statement = """  
        MERGE (c:__Community__ {community:value.community})  
        SET c += value {.level, .title, .rank, .rank_explanation, .full_content, .summary}  
        WITH c, value  
        UNWIND range(0, size(value.findings)-1) AS finding_idx  
        WITH c, value, finding_idx, value.findings[finding_idx] as finding  
        MERGE (c)-[:HAS_FINDING]->(f:Finding {id:finding_idx})  
        SET f += finding  
        """
        self.batched_import(community_statement, community_report_df)

        return True
    def find_newest_output_dir(self,):
        base_path = "./rag"
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
                    # 拆分目录名为日期和时间
                    date_part, time_part = subdir.split('-')
                    # 将日期部分转换为整数格式 YYYYMMDD
                    date_timestamp = int(date_part)
                    # 将时间部分转换为整数格式 HHMMSS
                    time_timestamp = int(time_part)
                    # 合并日期和时间成为唯一的时间戳
                    timestamp = date_timestamp * 1000000 + time_timestamp
                except (ValueError, IndexError):
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
                logging.info(f"Deleted folder: {tem_path}")

        return os.path.join(output_path, newest_dir)

