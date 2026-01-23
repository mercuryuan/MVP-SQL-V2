import streamlit as st
from src.pipeline.main_pipeline import SchemaLinkingPipeline
from src.utils.visualizer import plot_subgraph

st.title("Schema Linking Debugger")

db_name = st.selectbox("Select DB", ["bird_dev", "spider"])
question = st.text_input("Question", "Show me the students...")

if st.button("Step 1: SL1 Table Selection"):
    pipeline = SchemaLinkingPipeline(db_name)
    result = pipeline.run_sl1(question)

    st.write("Selected Tables:", result['selected_tables'])

    # 可视化当前选中的子图
    st.graphviz_chart(plot_subgraph(pipeline.graph_engine, result['selected_tables']))
