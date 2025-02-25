import streamlit as st
import pandas as pd
import json
import seaborn as sns
import matplotlib.pyplot as plt

from backend.consumer import KafkaConsumer

st.set_page_config(
    page_title="Real-Time Data Dashboard"
)

if "metric" not in st.session_state:
    st.session_state["metric"] = []
if "ratings" not in st.session_state:
    st.session_state["ratings"] = []

consumer = KafkaConsumer(
    config=
    {
        'bootstrap.servers': 'localhost:9095',
        "group.id": "visualization",
        "security.protocol": "PLAINTEXT"
    }
)
consumer.subscribe(topics='results')

st.title("Results")
chart_holder_metrics = st.empty()
chart_holder_histogram = st.empty()

while True:
    msgs = consumer.consume(num_messages=1, timeout=10)
    if msgs:
        for msg in msgs:
            if msg.error():
                print(f"Error in message: {msg.error()}")
                continue

            results_data = json.loads(msg.value().decode('utf-8'))

            st.session_state['metric'].append(results_data['metric'])
            st.session_state['ratings'].append(*results_data['rating'])

        df = pd.DataFrame.from_dict(st.session_state['metric'])
        chart_holder_metrics.line_chart(df)

        if len(st.session_state['ratings']) > 1:
            print(st.session_state['ratings'])
            fig, ax = plt.subplots(figsize=(8, 3))
            sns.histplot(st.session_state['ratings'], bins=10, kde=True, color='green', ax=ax)
            ax.set_title('Rating Distribution')
            ax.set_xlabel('Rating')
            ax.set_ylabel('Frequency')

            sns.set_style('darkgrid')

            chart_holder_histogram.pyplot(fig)

            plt.close(fig)
