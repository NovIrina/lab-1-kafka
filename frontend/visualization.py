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
if "popularities" not in st.session_state:
    st.session_state["popularities"] = []

consumer = KafkaConsumer(
    config=
    {
        'bootstrap.servers': 'localhost:9095',
        "group.id": "visualization",
        "security.protocol": "PLAINTEXT"
    }
)
consumer.subscribe(topics='results')

chart_holder_title = st.empty()
chart_holder_metrics = st.empty()
popularity_vs_rating_title = st.empty()
popularity_vs_rating = st.empty()

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
            st.session_state['popularities'].append(results_data['original']['features'][0]['popularity'])

        df = pd.DataFrame.from_dict(st.session_state['metric'])

        chart_holder_title.title("F1 Score")
        chart_holder_metrics.line_chart(df)

        df_2 = pd.DataFrame({
            'Predicted rating': st.session_state['ratings'],
            'Popularity': st.session_state['popularities']
        })

        popularity_vs_rating_title.title("Popularity vs Predicted rating")
        popularity_vs_rating.line_chart(df_2)
