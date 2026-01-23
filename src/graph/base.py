from abc import ABC, abstractmethod
from typing import List, Dict, Any


class GraphEngine(ABC):
    """
    图操作的抽象基类。
    上层 Schema Linking 逻辑 (SL1, SL2) 应仅依赖此接口。
    """

    @abstractmethod
    def load_graph(self, graph_path: str):
        """加载图数据"""
        pass

    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取表的属性（行数、主键等）"""
        pass

    @abstractmethod
    def get_columns_for_table(self, table_name: str) -> Dict[str, Dict]:
        """获取表的所有列及其详细统计信息"""
        pass

    @abstractmethod
    def get_neighbors(self, table_name: str, hop: int = 1) -> List[str]:
        """获取 N-hop 邻居表名"""
        pass

    @abstractmethod
    def get_shortest_path(self, source_table: str, target_table: str) -> List[str]:
        """获取两表之间的外键路径"""
        pass

    @abstractmethod
    def is_subgraph_connected(self, table_names: List[str]) -> bool:
        """判断选定的表集合是否连通"""
        pass
