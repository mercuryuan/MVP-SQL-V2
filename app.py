import streamlit as st
import networkx as nx
import tempfile
import os
import gc  # <--- 新增：用于强制垃圾回收
import time  # <--- 新增：用于等待系统释放锁
from decimal import Decimal
from datetime import date, datetime

# 引入 streamlit-agraph 组件
from streamlit_agraph import agraph, Node, Edge, Config

# 引入你的解析器
from schema_parser_nx import NetworkXSchemaParser

# ... (中间的辅助函数 format_value, nx_to_agraph_data 保持不变) ...

# ... (Streamlit 页面配置和侧边栏代码保持不变) ...

# --- 主区域 ---
if uploaded_file is not None:
    # 1. 保存临时文件
    # delete=False 是必须的，否则 Windows 下再次打开会报错，但我们需要手动清理
    with tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite") as tmp_file:
        tmp_file.write(uploaded_file.read())
        db_path = tmp_file.name

    # 初始化 parser 变量，防止后面 del 报错
    parser = None

    try:
        # 缓存解析过程
        # 注意：这里去掉了 @st.cache_resource，因为缓存可能会导致文件句柄被长期持有
        # 如果文件不大，直接解析即可。如果必须缓存，需要更复杂的 hash 策略
        def parse_db(path):
            p = NetworkXSchemaParser(path)
            p._build_graph_in_memory()
            return p, p.G  # 返回 parser 实例以便后续销毁


        # 获取 parser 实例和 图数据
        parser, G = parse_db(db_path)

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
            nodeHighlightBehavior=True,
            highlightColor="#F7A7A6",
            collapsible=False,
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

        # 5. 渲染图组件
        col_main, _ = st.columns([1, 0.01])
        with col_main:
            selected_id = agraph(nodes=nodes, edges=edges, config=config)

        # 6. 处理点击事件
        with details_container:
            st.header("🔍 Node Details")
            if selected_id:
                details = node_lookup.get(selected_id, {})
                node_type = details.get("type", "Unknown")
                node_name = details.get("name", selected_id)
                st.info(f"**Selected: {node_name}** ({node_type})")

                if node_type == "Table":
                    st.metric("Rows", details.get("row_count", 0))
                    st.write("**Columns List:**")
                    st.text(", ".join(details.get("columns", [])))
                elif node_type == "Column":
                    st.write(f"**Data Type:** `{details.get('data_type')}`")
                    if "samples" in details:
                        st.write("**Samples:**")
                        st.code(str(details["samples"]))
                    stats = {k: v for k, v in details.items() if
                             k not in ['samples', 'name', 'type', 'belongs_to', 'data_type']}
                    if stats:
                        st.write("**Statistics:**")
                        st.json(stats)
            else:
                st.write("👈 Click on a node in the graph to see its attributes here.")

    except Exception as e:
        st.error(f"Error: {e}")


    finally:

        # ========================================================

        # 终极修复：容错删除

        # ========================================================

        # 1. 主动断开引用，帮助 GC 识别垃圾

        parser = None

        if 'G' in locals():
            del G

        # 2. 强制垃圾回收（尝试释放句柄）

        gc.collect()

        # 3. 尝试删除，如果报错则直接忽略 (Pass)

        # Windows 上这非常常见，不要让它导致 App 崩溃

        if os.path.exists(db_path):

            try:

                os.remove(db_path)

            except PermissionError:

                # 记录日志到控制台（可选），但在网页上保持沉默

                print(f"[WinLock Warning] 无法立即删除临时文件 {db_path}，它将在系统清理时被移除。")

                pass  # <--- 关键：直接跳过，不报错

            except Exception as e:

                print(f"[Warning] 删除临时文件失败: {e}")

                pass
else:
    st.info("Please upload a database file from the sidebar.")
