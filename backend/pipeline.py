import traceback

from sklearn.metrics import f1_score
import joblib
import json
import pandas as pd

from consumer import KafkaConsumer
from producer import KafkaProducer


def main():
    print("Starting pipeline...")

    consumer = KafkaConsumer(
        config={
            'bootstrap.servers': 'localhost:9095',
            "group.id": "model",
            "security.protocol": "PLAINTEXT"
        }
    )
    consumer.subscribe(topics='processed_data')

    producer = KafkaProducer(bootstrap_servers='localhost:9095', topic='results')

    try:
        column_transformer = joblib.load("model/encoder.joblib")
        scaler = joblib.load("model/scaler.joblib")
        classifier = joblib.load("model/classifier.joblib")
        print("Pipeline components loaded successfully.")
    except Exception as e:
        print(f"Error loading pipeline components: {e}")
        return

    cumulative_y_true = []
    cumulative_y_pred = []
    processed_count = 0

    while True:
        msgs = consumer.consume(num_messages=1, timeout=10)
        if msgs:
            for msg in msgs:
                if msg.error():
                    print(f"Error in message: {msg.error()}")
                    continue
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    print(f'Pipeline received data: {data}')

                    X = pd.json_normalize(data["features"])
                    print(f"X: {X}")

                    X_transformed = column_transformer.transform(X)
                    X_transformed = pd.DataFrame(X_transformed, columns=column_transformer.get_feature_names_out())

                    columns_to_drop = [
                        "ohe__status_In Production",
                        "ohe__status_Planned",
                        "ohe__status_Rumored",
                        "remainder__runtime",
                        "remainder__adult",
                        "remainder__release_year"
                    ]
                    X_transformed.drop(columns=columns_to_drop, inplace=True, errors='ignore')

                    X_scaled = scaler.transform(X_transformed)

                    prediction = classifier.predict(X_scaled).tolist()

                    metrics = {
                        'rating': prediction,
                        'metric': {}
                    }

                    if "target" in data and data["target"] is not None:
                        y_true = data["target"]
                        if not isinstance(y_true, list):
                            y_true = [y_true]

                        if len(y_true) != len(prediction):
                            print("Warning: Mismatch in lengths of true labels and predictions.")

                        cumulative_y_true.extend(y_true)
                        cumulative_y_pred.extend(prediction)
                        processed_count += len(y_true)

                        f1 = f1_score(cumulative_y_true, cumulative_y_pred, average='weighted')
                        metrics['metric']['F1'] = f1
                        print(f"Cumulative F1 after {processed_count} entries: {f1}")
                    else:
                        f1 = None

                    producer.send_message(key='1', value=json.dumps(metrics))
                    print(f"Sent metrics: {metrics}")

                except Exception as e:
                    print(f"Error processing message: {e}")
                    traceback.print_exc()
        else:
            print("No messages received in pipeline.")


if __name__ == "__main__":
    main()
