from typing import List

import networkx as nx
import json
from .base import GraphEngine


class NetworkXExplorer(GraphEngine):
    def get_shortest_path(self, source_table: str, target_table: str) -> List[str]:
        """
        获取两表之间的最短路径（基于外键关系）。
        通常用于寻找 JOIN 路径。
        """
        try:
            # 同样转换为无向图，因为 JOIN 路径不关心外键定义的物理方向
            undirected_G = self.G.to_undirected()

            # 仅在 Table 类型的节点间寻找路径
            # NetworkX 的 shortest_path 会自动寻找，但需要确保路径上不经过 Column 节点
            # 由于你的构图逻辑中，Table 直接通过 FK 边连接 Table，
            # 且 Table 通过 HAS_COLUMN 连接 Column，
            # 所以 Table 到 Table 的最短路径通常就是 FK 链路，不会经过 Column。
            path = nx.shortest_path(undirected_G, source=source_table, target=target_table)

            # 过滤掉路径中可能的非 Table 节点（理论上按你的构图逻辑不会出现，但为了保险）
            return [node for node in path if self.G.nodes[node].get("type") == "Table"]
        except nx.NetworkXNoPath:
            return []
        except nx.NodeNotFound:
            return []

    def __init__(self, graph_path: str = None):
        self.G = None
        if graph_path:
            self.load_graph(graph_path)

    def load_graph(self, graph_path: str):
        """加载 JSON 格式的图数据"""
        with open(graph_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # NetworkX 原生支持读取 node-link 格式
        self.G = nx.node_link_graph(data)

    def get_table_info(self, table_name: str):
        if table_name not in self.G.nodes:
            return {}
        return self.G.nodes[table_name]

    def get_columns_for_table(self, table_name: str):
        columns = {}
        # 遍历邻居，找到类型为 HAS_COLUMN 的边连接的节点
        if table_name in self.G:
            for neighbor in self.G.successors(table_name):
                edge_data = self.G.get_edge_data(table_name, neighbor)
                # NetworkX 的边属性存储在 edge_data 中
                if edge_data.get("type") == "HAS_COLUMN":
                    col_node = self.G.nodes[neighbor]
                    columns[col_node["name"]] = col_node
        return columns

    def get_neighbors(self, table_name: str, hop: int = 1):
        """
        获取 N-hop 邻居。
        注意：NetworkX 的 neighbors() 包含出度和入度（如果是无向图），或者仅出度（有向图）。
        Schema Graph 通常是有向的 (FK)，但 Schema Linking 时我们通常把图视为无向的（A连B，B也连A）。
        """
        # 将图转为无向视图进行 BFS，因为外键关系在语义上是双向连通的
        undirected_G = self.G.to_undirected()

        # 使用 NetworkX 内置算法获取 k-hop 范围内的节点
        # single_source_shortest_path_length 返回 {node: distance}
        path_lengths = nx.single_source_shortest_path_length(undirected_G, table_name, cutoff=hop)

        # 过滤掉非 Table 类型的节点（例如 Column 节点）
        neighbor_tables = []
        for node, dist in path_lengths.items():
            if dist > 0 and self.G.nodes[node].get("type") == "Table":
                neighbor_tables.append(node)

        return neighbor_tables

    def is_subgraph_connected(self, table_names: list):
        if not table_names: return False
        # 提取子图
        sub_G = self.G.subgraph(table_names).to_undirected()
        # 过滤掉边类型不是 FOREIGN_KEY 的边 (可选，取决于你的图是否包含其他类型的表间边)
        # 这里假设 Table 之间只有 FK 边
        return nx.is_connected(sub_G)
