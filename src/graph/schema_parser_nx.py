import hashlib
import json
import operator
import os
import random
import sqlite3
from collections import Counter
from datetime import date
from datetime import datetime
from decimal import Decimal
# 添加此行
from dateutil import parser
import networkx as nx
import numpy as np


def convert_date_string(date_str):
    """
    尝试将输入的日期字符串按照多种常见格式转换为datetime对象或date对象。

    :param date_str: 日期字符串
    :return: 转换后的datetime对象或date对象，如果转换失败则返回None
    """
    # 检查 date_str 是否为字符串类型，如果不是则尝试转换为字符串
    # print(date_str)
    if not isinstance(date_str, str):
        try:
            date_str = str(date_str)
        except Exception as e:
            print(f"无法将输入 {date_str} 转换为字符串类型，错误信息: {e}")
            return None

    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y.%m.%d',
        '%m/%d/%Y',
        '%m-%d-%Y',
        '%d/%m/%Y',
        '%d-%m-%Y',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%Y.%m.%d %H:%M:%S',
        '%m/%d/%Y %H:%M:%S',
        '%m-%d-%Y %H:%M:%S',
        '%d/%m/%Y %H:%M:%S',
        '%d-%m-%Y %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',  # 新增的日期时间格式
        '%Y/%m/%d %H:%M:%S.%f',  # 新增的日期时间格式
        '%Y.%m.%d %H:%M:%S.%f',  # 新增的日期时间格式
        '%m/%d/%Y %H:%M:%S.%f',  # 新增的日期时间格式
        '%m-%d-%Y %H:%M:%S.%f',  # 新增的日期时间格式
        '%d/%m/%Y %H:%M:%S.%f',  # 新增的日期时间格式
        '%d-%m-%Y %H:%M:%S.%f',  # 新增的日期时间格式
        '%Y'  # 仅年份的格式
    ]
    for format_str in date_formats:
        try:
            dt = datetime.strptime(date_str, format_str)
            # 如果格式是纯日期（没有时间部分），返回 date 对象
            if format_str in ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d', '%m/%d/%Y', '%m-%d-%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y']:
                if format_str == '%Y':
                    # 对于仅年份的格式，将其转换为该年的 1 月 1 日
                    return date(dt.year, 1, 1)
                return dt.date()
            return dt
        except ValueError:
            continue
    # print(f"无法将日期字符串 {date_str} 转换为有效的日期时间格式，请检查数据格式！(大概率是问题数据，建议跳过)")
    return None


def quote_identifier(identifier):
    """
    引用标识符（表名或列名），防止包含空格或特殊字符时出错。

    :param identifier: 表名或列名
    :return: 引用后的标识符
    """
    return f'"{identifier}"'  # 使用双引号引用


def generate_fk_hash(table1, column1, table2, column2):
    """
    生成无序的外键ID，用于唯一标识外键关系。
    :param table1: 源表名
    :param column1: 源表列名
    :param table2: 目标表名
    :param column2: 目标表列名
    :return: 外键ID
    """
    elements = sorted([f"{table1}.{column1}", f"{table2}.{column2}"])  # 排序确保无序唯一
    unique_string = "|".join(elements)  # 连接字符串
    return hashlib.md5(unique_string.encode()).hexdigest()  # 生成哈希值


def _get_average_char_length(values):
    """
    计算文本型数据的平均字符长度。

    :param values: 文本数据列表
    :return: 平均字符长度
    """
    if values:
        total_length = sum(len(v) for v in values)
        return total_length / len(values)
    return 0


def _get_mode(values):
    """
    获取数值型或类别型数据的众数，全面考虑数据库数据中可能出现的多种情况，准确返回众数结果（支持返回多个众数情况）。

    :param values: 数据列表
    :return: 众数列表，如果不存在众数则返回空列表
    """
    if not values:
        return []

    # 处理高精度数值类型（如 DECIMAL）
    if all(isinstance(v, Decimal) for v in values):
        values = [float(v) for v in values]  # 将 Decimal 转换为 float 以便统计

    # 处理布尔类型
    if all(isinstance(v, bool) for v in values):
        values = [int(v) for v in values]  # 将布尔值转换为整数以便统计

    # 使用 Counter 统计每个元素出现的次数
    count_dict = Counter(values)
    max_count = max(count_dict.values())
    if max_count <= 1:
        return []
    # 找出所有出现次数等于最大次数的元素，即众数
    modes = [k for k, v in count_dict.items() if v == max_count]

    return modes


class NetworkXSchemaParser:
    def __init__(self, database_file: str):
        self.database_file = database_file
        # 核心改动：初始化一个 NetworkX 有向图，而不是 Neo4j Driver
        self.G = nx.DiGraph()
        self.conn = None

    def parse_and_save(self, output_path: str):
        """主入口：解析并保存为 JSON"""
        # 1. 执行解析逻辑（构建内存图）
        self._build_graph_in_memory()

        # 2. 序列化为 Node-Link JSON 格式
        graph_data = nx.node_link_data(self.G)

        # 3. 保存
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2, ensure_ascii=False)
        print(f"✅ 图数据已保存至: {output_path}")

    def _build_graph_in_memory(self):
        """
        对应原 parse_and_store_schema，但操作对象是 self.G
        """
        self.conn = sqlite3.connect(self.database_file)
        cursor = self.conn.cursor()
        try:

            # --- 1. 处理表 ---
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table in tables:
                table_name = table[0]
                if table_name == "sqlite_sequence": continue

                # 获取行数
                row_count = self._get_table_row_count(table_name)  # 复用原逻辑

                # [NX] 创建表节点
                self._create_table_node_nx(table_name, row_count)

                # --- 2. 处理列 ---
                cursor.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
                columns = cursor.fetchall()

                for column in columns:
                    col_name = column[1]
                    data_type = column[2]

                    # === 复用你原有的强大统计逻辑 ===
                    # 调用原有的 _get_column_samples_and_attributes 方法
                    # 注意：你需要把原文件中的这个方法复制到这个类里，或者继承原类
                    limit = 100000 if row_count > 100000 else None
                    samples, attrs = self._get_column_samples_and_attributes(table_name, col_name, data_type, limit)

                    # [NX] 创建列节点和边
                    self._create_column_node_nx(table_name, col_name, data_type, samples, attrs)

            # --- 3. 处理外键 ---
            for table in tables:
                table_name = table[0]
                if table_name == "sqlite_sequence": continue

                cursor.execute(f"PRAGMA foreign_key_list({quote_identifier(table_name)})")
                fks = cursor.fetchall()
                for fk in fks:
                    # SQLite PRAGMA return: id, seq, table, from, to, on_update, on_delete, match
                    to_table = fk[2]
                    from_column = fk[3]
                    to_column = fk[4]

                    # [NX] 创建外键边
                    self._create_fk_edge_nx(table_name, from_column, to_table, to_column)
        finally:

            cursor.close()
            self.conn.close()
            self.conn = None

    def _create_table_node_nx(self, table_name, row_count):
        """替代原 _create_table_node_in_neo4j"""
        # 获取主键外键等信息 (复用原逻辑)
        pk = self._get_primary_key_columns(table_name)
        fk = self._get_foreign_key_columns(table_name)
        cols = self._get_table_columns(table_name)

        attrs = {
            "type": "Table",  # 显式标记节点类型
            "name": table_name,
            "row_count": row_count,
            "primary_key": pk,
            "columns": cols
        }
        # NetworkX 添加节点
        self.G.add_node(table_name, **attrs)

    def _create_column_node_nx(self, table_name, col_name, data_type, samples, additional_attrs):
        """替代原 _create_column_node_and_relation_in_neo4j"""
        node_id = f"{table_name}.{col_name}"

        attrs = {
            "type": "Column",
            "name": col_name,
            "data_type": data_type,
            "samples": samples,
            "belongs_to": table_name
        }
        # 合并统计属性 (null_count, distinct values等)
        attrs.update(additional_attrs)

        # 1. 添加列节点
        self.G.add_node(node_id, **attrs)

        # 2. 添加 Table -> Column 的边
        # 你的原逻辑区分了 relationship_type，这里作为边的属性存储
        rel_type = "normal_column"
        if "primary_key" in attrs.get("key_type", []): rel_type = "primary_key"

        self.G.add_edge(table_name, node_id, type="HAS_COLUMN", relation_type=rel_type)

    def _create_fk_edge_nx(self, from_table, from_col, to_table, to_col):
        """替代原 _create_foreign_key_relations_in_neo4j"""
        # 如果 to_col 为空，查找目标表主键 (复用原逻辑)
        if to_col is None:
            to_col = self._get_primary_key_columns(to_table)[0]

        fk_path = f"{from_table}.{from_col}={to_table}.{to_col}"

        # [NX] NetworkX 的边可以直接包含属性
        # 注意：这里我们直接在 Table 节点之间建立外键边，方便 BFS 搜索
        self.G.add_edge(from_table, to_table,
                        type="FOREIGN_KEY",
                        from_column=from_col,
                        to_column=to_col,
                        reference_path=fk_path)

    def _get_table_row_count(self, table_name):
        """
        获取指定表的数据条数。

        :param table_name: 表名
        :return: 表的数据条数，如果获取失败返回None
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {quote_identifier(table_name)}")
            row_count = cursor.fetchone()[0]
            return row_count
        except Exception as e:
            print(f"获取表 {table_name} 数据条数时出错: {e}")
            return None

    def _get_table_columns(self, table_name):
        """
        获取指定表包含的所有列名列表。

        :param table_name: 表名
        :return: 列名列表
        """

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
        columns_info = cursor.fetchall()
        columns = [column_info[1] for column_info in columns_info]
        return columns

    def _get_column_samples_and_attributes(self, table_name, column_name, data_type, sample_size=None):
        """
        随机抽样获取列的数据样本，并计算附加属性（范围或类别等多种属性）。
        实现为所有类型的列节点添加数据条数属性，对于非id主键的数值型数据添加平均数等相关属性，
        同时根据数据库设计判断列是否为主键或外键并统一添加相应属性到key_type。
        对传入的数据类型进行统一处理，正确识别类似VARCHAR(50)这种带长度限定的数据类型为对应的基础类型（文本类型），以便后续属性计算。
        新增列是否可为空和列是否可重复的属性。

        :param table_name: 表名
        :param column_name: 列名
        :param data_type: 列的数据类型（可能包含类似VARCHAR(50)带长度限定的格式）
        :return: 列的随机抽样数据和按照特定命名规则组织的附加属性字典
        """
        samples = []
        additional_attributes = {}
        # 处理数据类型，去除括号及里面的长度限定部分，统一为基础类型名称
        base_data_type = data_type.split('(')[0].upper()

        cursor = self.conn.cursor()
        # 查询列数据
        if sample_size is not None:
            # 如果指定了抽样个数，则限制查询结果
            query = f"SELECT {quote_identifier(column_name)} FROM {quote_identifier(table_name)} LIMIT {sample_size};"
        else:
            # 否则查询全部数据
            query = f"SELECT {quote_identifier(column_name)} FROM {quote_identifier(table_name)} ;"

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            all_values = [row[0] for row in rows]
        except sqlite3.OperationalError as e:
            print(f"警告：读取 {table_name}.{column_name} 失败，尝试使用 bytes 方式处理，错误信息：{e}")

            # 重新连接，并使用 text_factory=bytes
            with sqlite3.connect(self.database_file) as conn_fallback:
                conn_fallback.text_factory = bytes
                cursor_fallback = conn_fallback.cursor()
                cursor_fallback.execute(query)
                rows = cursor_fallback.fetchall()

                # 逐行处理可能的 bytes 数据
                all_values = []
                for row in rows:
                    value = row[0]
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='ignore')  # 忽略无法解码的字符
                    all_values.append(value)

            # 过滤空值（包括 None、空字符串和仅含空格的字符串）
            def is_empty(value):
                return value is None or (isinstance(value, str) and value.strip() == "")

            non_null_values = [v for v in all_values if not is_empty(v)]

            # 数据完整性统计
            null_count = len(all_values) - len(non_null_values)
            additional_attributes['null_count'] = null_count
            # 计算数据完整性并以百分比形式存储
            additional_attributes['data_integrity'] = "{:.0f}%".format(
                len(non_null_values) / len(all_values) * 100) if all_values else "100%"

            # if null_count > 0:
            #     print(f"过滤掉了 {table_name} 中 {column_name} 的 {null_count} 个空值数据")
            numeric_types = [
                "INTEGER", "INT", "SMALLINT", "BIGINT", "TINYINT", "MEDIUMINT",  # 整数类型
                "REAL", "FLOAT", "DOUBLE",  # 浮点数类型
                "DECIMAL", "NUMERIC",  # 高精度小数类型
                "BOOLEAN"  # 布尔类型
            ]
            text_types = [
                "TEXT", "VARCHAR", "CHAR", "NCHAR", "NVARCHAR", "NTEXT",  # 常见文本类型
                "CLOB", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT",  # 大文本类型
                "JSON", "XML"  # 结构化文本类型
            ]
            # 获取随机样本，最多取6条（如果数据量小于等于5则取全部）
            if base_data_type in text_types:
                samples = random.sample(non_null_values, min(len(non_null_values), 6))
                #  文本类的采样可能过长，如果单个采样过长则截断，过长部分采用省略号替换
                max_length = 30  # 最大长度限制
                samples = [s[:max_length] + '...' if len(s) > max_length else s for s in samples]


            else:
                samples = random.sample(non_null_values, min(len(non_null_values), 6))

            # 为所有类型列节点添加抽样个数属性
            additional_attributes['sample_count'] = len(non_null_values) if non_null_values else 0

            # 查询列是否可为空
            is_nullable = self._is_column_nullable(table_name, column_name)
            additional_attributes['is_nullable'] = is_nullable

            # 查询列是否为主键
            is_primary_key = self._is_primary_key(table_name, column_name)
            # 查询列是否为外键
            is_foreign_key = self._is_foreign_key(table_name, column_name)

            if base_data_type in numeric_types:
                # 过滤掉非数值型数据
                valid_values = [v for v in non_null_values if isinstance(v, (int, float)) and v != '']
                filtered_count = len(non_null_values) - len(valid_values)
                if filtered_count > 0:
                    print(f"过滤掉了 {table_name} 中 {column_name} 的 {filtered_count} 个非数值数据")

                try:
                    # 计算数值范围
                    additional_attributes['numeric_range'] = [min(valid_values),
                                                              max(valid_values)] if valid_values else None

                    is_id_column = "id" in column_name.lower()
                    if not is_id_column:
                        # 计算众数
                        mode = _get_mode(valid_values)
                        # 若次数大于1次的众数存在，则将其添加到附加属性中
                        if mode:
                            additional_attributes['numeric_mode'] = mode

                        # 计算平均值
                        if valid_values:
                            try:
                                if base_data_type in ["DECIMAL", "NUMERIC"]:
                                    additional_attributes['numeric_mean'] = float(
                                        np.mean([float(Decimal(str(v))) for v in valid_values]))
                                elif base_data_type == "BOOLEAN":
                                    additional_attributes['numeric_mean'] = float(
                                        np.mean([int(v) for v in valid_values]))
                                else:
                                    additional_attributes['numeric_mean'] = float(np.mean(valid_values))
                            except Exception as e:
                                print(f"计算平均值时出错: {e}")
                                print(f"表名: {table_name}, 列名: {column_name}")
                                additional_attributes['numeric_mean'] = None
                        else:
                            additional_attributes['numeric_mean'] = None
                except Exception as e:
                    print(f"计算数值范围时出错: {e}")
                    print(f"表名: {table_name}, 列名: {column_name}")
                    additional_attributes['numeric_range'] = None

            elif base_data_type in text_types:
                # 如果唯一值数量小于等于 6，将其视为类别型数据
                if len(set(non_null_values)) <= 6:
                    additional_attributes['text_categories'] = list(set(non_null_values))

                # 计算平均字符长度
                additional_attributes['average_char_length'] = _get_average_char_length(
                    non_null_values) if non_null_values else 0

                # 计算词频属性，并转换为 JSON 字符串
                word_frequency_dict = self._get_word_frequency(non_null_values) if non_null_values else {}
                additional_attributes['word_frequency'] = json.dumps(word_frequency_dict, ensure_ascii=False)

            elif base_data_type in ["DATE", "DATETIME", "TIMESTAMP"]:
                additional_attributes['time_span'] = self._get_time_span(non_null_values) if non_null_values else None
                time_attributes = self.calculate_time_attributes(non_null_values)
                additional_attributes.update(time_attributes)

            # 根据主键或外键查询结果添加key_type属性（仅针对主键或外键列）
            key_type = []
            if is_primary_key:
                key_type.append("primary_key")
            if is_foreign_key:
                key_type.append("foreign_key")
            if key_type:
                additional_attributes['key_type'] = key_type

        return samples, additional_attributes

    def _get_primary_key_columns(self, table_name):
        """
        获取指定表的主键列信息（以 SQLite 为例）。

        :param table_name: 表名
        :return: 主键列名列表，如果是单个主键则返回只包含该列名的列表，无主键返回空列表
        """
        try:
            cursor = self.conn.cursor()
            sql_statement = f"PRAGMA table_info({quote_identifier(table_name)})"
            cursor.execute(sql_statement)
            columns_info = cursor.fetchall()
            primary_key_columns = []
            for column_info in columns_info:
                if column_info[5] != 0:  # 假设第 6 个元素（索引为 5）表示是否为主键，1 为主键，0 为非主键，不同数据库该位置可能不同
                    primary_key_columns.append(column_info[1])  # 第 2 个元素（索引为 1）是列名
            return primary_key_columns
        except sqlite3.Error as e:
            print(f"Error occurred while executing SQL statement: {sql_statement}")
            print(f"Error message: {e}")
            return []

    def _get_foreign_key_columns(self, table_name):
        """
        获取指定表的外键列信息（以 SQLite 为例）。

        :param table_name: 表名
        :return: 外键列名列表，如果是单个外键则返回只包含该列名的列表，无外键返回空列表
        """
        try:

            cursor = self.conn.cursor()
            sql_statement = f"PRAGMA foreign_key_list({quote_identifier(table_name)})"
            cursor.execute(sql_statement)
            foreign_keys = cursor.fetchall()
            foreign_key_columns = [fk[3] for fk in foreign_keys]  # 第 4 个元素（索引为 3）是本地列名，对应外键列
            return foreign_key_columns
        except sqlite3.Error as e:
            print(f"Error occurred while executing SQL statement: {sql_statement}")
            print(f"Error message: {e}")
            return []

    def _is_column_nullable(self, table_name, column_name):
        """
        判断列是否可为空。

        :param table_name: 表名
        :param column_name: 列名
        :return: True 表示列可为空，False 表示列不允许为空
        """

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
        columns_info = cursor.fetchall()
        for column_info in columns_info:
            if column_info[1] == column_name:  # 第2个元素（索引为1）是列名
                notnull = column_info[3]  # 第4个元素（索引为3）表示是否允许为空，0 表示允许为空，1 表示不允许为空
                return not notnull  # 当 notnull 为 0 时返回 True，为 1 时返回 False
        return None

    def _is_primary_key(self, table_name, column_name):
        """
        判断指定表中的指定列是否为主键，通过查询数据库元数据（以SQLite为例）。

        :param table_name: 表名
        :param column_name: 列名
        :return: True如果是主键，False否则
        """

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({quote_identifier(table_name)})")
        columns_info = cursor.fetchall()
        for column_info in columns_info:
            if column_info[1] == column_name:  # 第2个元素（索引为1）是列名
                return bool(column_info[5])  # 第6个元素（索引为5）表示是否为主键，1为主键，0为非主键，不同数据库该位置可能不同
        return False

    def _is_foreign_key(self, table_name, column_name):
        """
        判断指定表中的指定列是否为外键，通过查询数据库元数据（以SQLite为例）。

        :param table_name: 表名
        :param column_name: 列名
        :return: True如果是外键，False否则
        """

        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA foreign_key_list({quote_identifier(table_name)})")
        foreign_keys = cursor.fetchall()
        for foreign_key in foreign_keys:
            if foreign_key[3] == column_name:  # 第4个元素（索引为3）是本地列名，对应外键列
                return True
        return False

    def _get_word_frequency(self, values, top_k=10, by_word=False):
        """
        统计文本型数据的词频，支持按词语或整个值为单位统计，并返回前 top_k 个高频词。

        :param values: 文本数据列表，函数将对该列表中的文本数据进行词频统计。若列表为空，则直接返回空字典。
        :param top_k: 返回的高频词数量，默认值为 10。在统计完成后，函数将按照词频从高到低排序，尝试取前 top_k 个高频词作为结果。
        :param by_word: 是否以词语为单位统计词频，默认值为 False。
            - 若为 True，则会将文本数据拆分为单个词语，以每个词语为单位统计词频。
            - 若为 False，则以整个文本值为单位统计词频。
        :return: 词频字典，格式为 {"word": frequency}，包含前 top_k 个高频词且按频率从高到低排序。
            特殊情况：从第一个频率为 1 的词开始，最多保留三个频率为 1 的词，且词频为 1 的词长度不能超过 20。
        """
        # 检查输入的文本数据列表是否为空
        if not values:
            return {}

        # 统计词频
        if by_word:
            # 以词语为单位统计
            # 将文本数据列表中的所有文本连接成一个长字符串，再按空格拆分为单个单词
            all_words = " ".join(values).split()
            word_count_dict = Counter(all_words)
        else:
            # 以整个值为单位统计
            word_count_dict = Counter(values)

        # 按照词频从高到低排序
        sorted_word_count_dict = dict(
            sorted(word_count_dict.items(), key=operator.itemgetter(1), reverse=True)
        )

        result = {}
        one_freq_count = 0
        found_one_freq = False

        for word, freq in sorted_word_count_dict.items():
            if len(result) >= top_k:
                break
            if freq == 1:
                found_one_freq = True
                if len(word) <= 20 and one_freq_count < 3:
                    result[word] = freq
                    one_freq_count += 1
            else:
                if not found_one_freq:
                    result[word] = freq

        return result

    def _get_time_span(self, values):
        """
        计算时间类型数据的时间跨度，兼容多种日期时间格式的数据。

        :param values: 时间数据列表
        :return: 时间跨度描述字符串（示例，可按需求调整格式）
        """
        if values:
            datetime_values = [convert_date_string(v) for v in values if convert_date_string(v) is not None]
            if datetime_values:
                min_time = min(datetime_values)
                max_time = max(datetime_values)
                time_diff = max_time - min_time
                return f"{time_diff.days} days"
        return None

    def calculate_time_attributes(self, values):
        """
        计算给定时间数据列表中的最早时间和最晚时间属性，根据不同时间数据类型进行针对性处理。

        参数:
        values (list): 包含时间数据的列表，时间数据格式可能多样，例如 '2025-01-01'（date类型）、'2025-01-01 12:30:00'（datetime类型）、
                       '1983-12-29T00:00:00'（类似timestamp格式等）等。

        返回:
        dict: 包含最早时间（'earliest_time'）和最晚时间（'latest_time'）属性的字典，如果输入列表为空则对应属性值为 None。
        """
        parsed_values = []
        for v in values:
            if isinstance(v, str):
                if "T" in v:
                    # 处理类似timestamp格式
                    try:
                        parsed = parser.isoparse(v)
                    except ValueError:
                        parsed = None
                else:
                    # 处理普通日期时间格式
                    parsed = convert_date_string(v)
            elif isinstance(v, datetime):
                parsed = v
            elif isinstance(v, date):
                parsed = v  # 保留 date 对象
            else:
                parsed = None

            if parsed:
                parsed_values.append(parsed)

        if parsed_values:
            # 根据类型决定格式化方式
            def format_value(value):
                if isinstance(value, date) and not isinstance(value, datetime):
                    return value.strftime('%Y-%m-%d')  # 只格式化日期部分
                else:
                    return value.strftime('%Y-%m-%d %H:%M:%S')  # 格式化日期和时间部分

            time_attributes = {
                'earliest_time': format_value(min(parsed_values)),
                'latest_time': format_value(max(parsed_values))
            }
        else:
            time_attributes = {
                'earliest_time': None,
                'latest_time': None
            }

        return time_attributes

    def extract_dataset_name(self, database_file):
        possible_datasets = ['bird', 'spider', 'BIRD']
        for dataset_name in possible_datasets:
            if dataset_name in database_file:
                return dataset_name
        return None

    def extract_database_name(self, database_file):
        """
        从给定的数据库文件路径中提取数据库名称。

        :param database_file: 数据库文件的路径
        :return: 数据库名称（不带路径和扩展名）
        """
        # 使用 os.path.basename 获取文件名（带扩展名）
        file_name = os.path.basename(database_file)
        # 使用 os.path.splitext 去除扩展名
        database_name = os.path.splitext(file_name)[0]
        return database_name
