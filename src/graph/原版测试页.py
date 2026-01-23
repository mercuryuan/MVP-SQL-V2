import streamlit as st
import networkx as nx
from pyvis.network import Network
import tempfile
import os
import streamlit.components.v1 as components

# 引入你上传的模块
# 确保 schema_parser_nx.py, nx_explorer.py, base.py 在同一目录下
from schema_parser_nx import NetworkXSchemaParser


# ==========================================
# 1. 辅助函数：清洗数据以适配可视化
# ==========================================
def clean_graph_for_vis(G):
    """
    Pyvis/JSON 序列化不支持 Decimal, datetime, bytes 等类型。
    在可视化前，我们需要将这些属性转为字符串或浮点数。
    """
    from decimal import Decimal
    from datetime import date, datetime

    # 创建副本以免修改原始图数据
    vis_G = G.copy()

    for node_id, attrs in vis_G.nodes(data=True):
        for k, v in attrs.items():
            if isinstance(v, (Decimal)):
                attrs[k] = float(v)
            elif isinstance(v, (date, datetime)):
                attrs[k] = str(v)
            elif isinstance(v, bytes):
                attrs[k] = "<binary>"
            elif isinstance(v, list):
                # 简化列表显示，防止弹窗太长
                attrs[k] = str(v)[:100] + "..." if len(str(v)) > 100 else str(v)
            elif v is None:
                attrs[k] = ""

    # 同样处理边属性
    for u, v, attrs in vis_G.edges(data=True):
        for k, val in attrs.items():
            if isinstance(val, list):
                attrs[k] = str(val)

    return vis_G


def apply_neo4j_style(G):
    """
    为节点应用类似 Neo4j 的样式：
    - Table: 蓝色，大圆点
    - Column: 黄色/灰色，小圆点
    - Edge: 只有 Foreign Key 显示箭头和标签
    """
    for node_id, attrs in G.nodes(data=True):
        node_type = attrs.get("type", "Unknown")

        # === 节点样式 ===
        if node_type == "Table":
            attrs['color'] = '#4D88FF'  # Neo4j 经典蓝
            attrs['size'] = 30
            attrs['title'] = f"Table: {attrs.get('name')}\nRows: {attrs.get('row_count')}"
            attrs['label'] = attrs.get('name')  # 节点上显示的文字
            attrs['font'] = {'size': 20, 'color': 'white'}

        elif node_type == "Column":
            # 区分主键和普通列
            if "primary_key" in attrs.get("key_type", []):
                attrs['color'] = '#FFD700'  # 金色主键
                attrs['size'] = 15
            elif "foreign_key" in attrs.get("key_type", []):
                attrs['color'] = '#FF9900'  # 橙色外键
                attrs['size'] = 12
            else:
                attrs['color'] = '#CCCCCC'  # 灰色普通列
                attrs['size'] = 10

            # 鼠标悬停显示的 Tooltip
            attrs['title'] = (
                f"Column: {attrs.get('name')}\n"
                f"Type: {attrs.get('data_type')}\n"
                f"Sample: {str(attrs.get('samples', []))[:50]}..."
            )
            # 默认不显示列名 label，防止图太乱，除非鼠标悬停（Pyvis默认逻辑）
            # 或者我们可以选择只显示列名
            attrs['label'] = attrs.get('name')

            # === 边样式 ===
    for u, v, attrs in G.edges(data=True):
        edge_type = attrs.get("type")

        if edge_type == "FOREIGN_KEY":
            attrs['color'] = '#FF4500'  # 红色连线
            attrs['width'] = 2
            attrs['label'] = "FK"  # 线上显示 FK
            attrs['arrows'] = 'to'  # 箭头
            # 悬停显示详情
            attrs['title'] = f"FK: {attrs.get('from_column')} -> {attrs.get('to_column')}"

        elif edge_type == "HAS_COLUMN":
            attrs['color'] = '#999999'
            attrs['width'] = 1
            attrs['arrows'] = ''  # 内部关系不加箭头，减少视觉干扰

    return G


# ==========================================
# 2. Streamlit 页面布局
# ==========================================
st.set_page_config(page_title="Schema Graph Visualizer", layout="wide")

st.title("🕸️ SQLite Schema Graph Explorer")
st.markdown("上传 SQLite 数据库，生成类似 **Neo4j** 的物理交互图结构。")

# 侧边栏：控制面板
with st.sidebar:
    st.header("1. Upload Database")
    uploaded_file = st.file_uploader("Upload .sqlite or .db file", type=["sqlite", "db"])

    st.header("2. Visualization Settings")
    view_mode = st.radio("View Mode", ["Full Schema (Tables + Columns)", "Table Relationships Only (Simplified)"])

    physics_enabled = st.checkbox("Enable Physics (Wobbly effect)", value=True)

    st.info("💡 Tip: 'Table Relationships Only' is better for large databases.")

# 主逻辑
if uploaded_file is not None:
    # 1. 将上传的文件保存为临时文件，因为 Parser 需要文件路径
    with tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite") as tmp_file:
        tmp_file.write(uploaded_file.read())
        db_path = tmp_file.name

    try:
        # 2. 解析图结构
        with st.spinner("Parsing Database Schema..."):
            # 实例化你的解析器
            parser = NetworkXSchemaParser(db_path)
            # 构建内存图 (注意：这里调用了你类中的 _build_graph_in_memory)
            # 由于你的代码原本是在 parse_and_save 里调用的，我们这里手动调用
            parser._build_graph_in_memory()
            G = parser.G

        st.success(f"✅ Graph Generated! Nodes: {len(G.nodes)}, Edges: {len(G.edges)}")

        # 3. 根据用户选择过滤图
        if view_mode == "Table Relationships Only (Simplified)":
            # 只保留 Table 类型的节点
            table_nodes = [n for n, attr in G.nodes(data=True) if attr.get("type") == "Table"]
            G_sub = G.subgraph(table_nodes).copy()
            # 此时边也会自动保留 Table 之间的 FK 边
        else:
            G_sub = G.copy()

        # 4. 样式美化 & 数据清洗
        G_styled = apply_neo4j_style(G_sub)
        G_clean = clean_graph_for_vis(G_styled)

        # 5. 使用 Pyvis 生成可视化
        # height 设置画布高度
        net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white", notebook=False)

        # 将 NetworkX 图加载进 Pyvis
        net.from_nx(G_clean)

        # 配置物理引擎效果
        if physics_enabled:
            net.toggle_physics(True)
            # 使用 barnesHut 算法，适合类似 Neo4j 的网络拓扑
            net.barnes_hut(gravity=-8000, central_gravity=0.3, spring_length=200, spring_strength=0.001, damping=0.09,
                           overlap=0)
        else:
            net.toggle_physics(False)

        # 添加控制按钮（可选，让用户自己调物理参数）
        # net.show_buttons(filter_=['physics'])

        # 6. 渲染到 Streamlit
        # Pyvis 生成 HTML
        with tempfile.NamedTemporaryFile(delete=False, suffix=".html") as tmp_html:
            net.save_graph(tmp_html.name)

            # 读取 HTML 内容
            with open(tmp_html.name, 'r', encoding='utf-8') as f:
                html_source = f.read()

            # 使用 Streamlit 组件展示
            st.components.v1.html(html_source, height=800, scrolling=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")
        st.code(str(e))  # 显示具体报错堆栈

    finally:
        # 清理临时数据库文件
        os.remove(db_path)
else:
    st.write("👈 Please upload a database file from the sidebar to start.")
