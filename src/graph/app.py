import streamlit as st
import networkx as nx
import tempfile
import os
import json
from decimal import Decimal
from datetime import date, datetime

# 引入 streamlit-agraph 组件
from streamlit_agraph import agraph, Node, Edge, Config

# 引入你的解析器
from schema_parser_nx import NetworkXSchemaParser


# ==========================================
# 1. 辅助函数：样式与转换
# ==========================================

def format_value(v):
    """格式化属性值用于展示，处理无法直接序列化的类型"""
    if isinstance(v, (Decimal)):
        return float(v)
    elif isinstance(v, (date, datetime)):
        return str(v)
    elif isinstance(v, bytes):
        return "<binary data>"
    elif isinstance(v, list):
        return str(v)
    return v


def nx_to_agraph_data(G):
    """
    将 NetworkX 图转换为 agraph 需要的 Node 和 Edge 列表。
    同时返回一个 node_details 字典，用于点击后查找属性。
    """
    nodes = []
    edges = []
    node_details = {}  # 用于存储原始属性，供侧边栏展示

    # --- 1. 处理节点 ---
    for node_id, attrs in G.nodes(data=True):
        node_type = attrs.get("type", "Unknown")
        label_text = attrs.get("name", node_id)

        # 截断过长的名字以保持圆形美观
        display_label = label_text if len(label_text) < 15 else label_text[:12] + "..."

        # 存储详细信息到字典（清洗数据以防报错）
        clean_attrs = {k: format_value(v) for k, v in attrs.items()}
        node_details[node_id] = clean_attrs

        # 样式配置
        if node_type == "Table":
            # 表节点：蓝色，大一点
            nodes.append(Node(
                id=node_id,
                label=display_label,
                size=30,
                shape="ellipse",  # ellipse 形状会将文字包裹在内部
                color="#4D88FF",
                font={"color": "white", "size": 16}
            ))
        elif node_type == "Column":
            # 列节点
            is_pk = "primary_key" in attrs.get("key_type", [])
            is_fk = "foreign_key" in attrs.get("key_type", [])

            if is_pk:
                color = "#FFD700"  # 金色主键
                size = 20
                font_color = "black"
            elif is_fk:
                color = "#FF9900"  # 橙色外键
                size = 18
                font_color = "black"
            else:
                color = "#E0E0E0"  # 灰色普通列
                size = 15
                font_color = "gray"

            nodes.append(Node(
                id=node_id,
                label=display_label,
                size=size,
                shape="ellipse",  # 文字在内部
                color=color,
                font={"color": font_color, "size": 12}
            ))

    # --- 2. 处理边 ---
    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get("type")

        if edge_type == "FOREIGN_KEY":
            edges.append(Edge(
                source=u,
                target=v,
                label="FK",
                color="#FF4500",
                width=2,
            ))
        elif edge_type == "HAS_COLUMN":
            edges.append(Edge(
                source=u,
                target=v,
                color="#CCCCCC",
                width=1
            ))

    return nodes, edges, node_details


# ==========================================
# 2. Streamlit 页面逻辑
# ==========================================
st.set_page_config(page_title="Schema Graph Visualizer", layout="wide")

# 初始化 Session State 用于存储当前选中的节点
if 'selected_node_id' not in st.session_state:
    st.session_state['selected_node_id'] = None

st.title("🕸️ Interactive Schema Explorer")

# --- 侧边栏：控制与详情 ---
with st.sidebar:
    st.header("📂 1. Control Panel")
    uploaded_file = st.file_uploader("Upload SQLite DB", type=["sqlite", "db"])

    view_mode = st.radio(
        "View Mode",
        ["Table Relationships Only (Simplified)", "Full Schema (Tables + Columns)"]
    )

    st.markdown("---")

    # 动态属性面板占位符
    details_container = st.container()

# --- 主区域 ---
if uploaded_file is not None:
    # 1. 保存并解析文件
    with tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite") as tmp_file:
        tmp_file.write(uploaded_file.read())
        db_path = tmp_file.name

    try:
        # 缓存解析过程，防止每次点击都重新解析数据库
        @st.cache_resource
        def parse_db(path):
            parser = NetworkXSchemaParser(path)
            parser._build_graph_in_memory()
            return parser.G


        G = parse_db(db_path)

        # 2. 根据视图模式过滤图
        if view_mode == "Table Relationships Only (Simplified)":
            nodes_subset = [n for n, attr in G.nodes(data=True) if attr.get("type") == "Table"]
            G_vis = G.subgraph(nodes_subset)
        else:
            G_vis = G

        # 3. 转换数据为 Agraph 格式
        nodes, edges, node_lookup = nx_to_agraph_data(G_vis)

        # 4. 配置可视化参数
        config = Config(
            width="100%",
            height=700,
            directed=True,
            physics=True,
            hierarchy=False,
            nodeHighlightBehavior=True,  # 允许高亮
            highlightColor="#F7A7A6",
            collapsible=False,
            # 配置物理引擎，让图动起来但不要太乱
            physics_settings={
                "barnesHut": {
                    "gravitationalConstant": -3000,
                    "centralGravity": 0.3,
                    "springLength": 200,
                    "springConstant": 0.05,
                    "damping": 0.09,
                    "avoidOverlap": 0.5
                },
                "minVelocity": 0.75
            }
        )

        # 5. 渲染图组件并捕获返回值
        # return_value 会是用户点击的节点的 id
        col_main, _ = st.columns([1, 0.01])  # 占满主屏

        with col_main:
            selected_id = agraph(nodes=nodes, edges=edges, config=config)

        # 6. 处理点击事件 (在 Sidebar 显示详情)
        with details_container:
            st.header("🔍 Node Details")

            if selected_id:
                # 从查找表中获取属性
                details = node_lookup.get(selected_id, {})
                node_type = details.get("type", "Unknown")
                node_name = details.get("name", selected_id)

                # 顶部高亮显示名字
                st.info(f"**Selected: {node_name}** ({node_type})")

                # 使用 JSON 或 表格 展示属性
                if node_type == "Table":
                    st.metric("Rows", details.get("row_count", 0))
                    st.write("**Columns List:**")
                    st.text(", ".join(details.get("columns", [])))

                elif node_type == "Column":
                    st.write(f"**Data Type:** `{details.get('data_type')}`")

                    # 展示样本数据
                    if "samples" in details:
                        st.write("**Samples:**")
                        st.code(str(details["samples"]))

                    # 展示统计信息
                    stats = {k: v for k, v in details.items() if
                             k not in ['samples', 'name', 'type', 'belongs_to', 'data_type']}
                    if stats:
                        st.write("**Statistics:**")
                        st.json(stats)
            else:
                st.write("👈 Click on a node in the graph to see its attributes here.")

    except Exception as e:
        st.error(f"Error: {e}")
        # st.exception(e) # 开发调试时打开
    finally:
        # 清理
        try:
            os.remove(db_path)
        except:
            pass

else:
    st.info("Please upload a database file from the sidebar.")
