import pandas as pd
import json

from consumer import KafkaConsumer
from producer import KafkaProducer


def remove_nan_and_duplicates(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.dropna(inplace=True)
    dataset.drop_duplicates(inplace=True)
    return dataset


def remove_unnecessary_columns(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.drop(columns=[
        "title", "imdb_id", "original_language", "original_title",
        "overview", "tagline", "genres", "production_companies",
        "production_countries", "spoken_languages", "keywords"
    ], inplace=True)
    return dataset


def convert_features(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset['adult'] = dataset['adult'].astype(int)
    dataset['release_date'] = pd.to_datetime(dataset['release_date'], errors='coerce')
    dataset.dropna(inplace=True)
    dataset['release_year'] = dataset['release_date'].dt.year
    dataset.drop(columns='release_date', inplace=True)
    return dataset


def main():
    print("Starting preprocessing...")
    consumer = KafkaConsumer(
        config={
            'bootstrap.servers': 'localhost:9095',
            "group.id": "data_processors",
            "security.protocol": "PLAINTEXT"
        }
    )
    consumer.subscribe(topics="raw_data")
    producer = KafkaProducer(bootstrap_servers='localhost:9095',
                             topic='processed_data')
    while True:
        msgs = consumer.consume(num_messages=1, timeout=10)
        if msgs:
            for msg in msgs:
                if msg.error():
                    print(f"Error in message: {msg.error()}")
                    continue
                try:
                    dataset = json.loads(msg.value().decode('utf-8'))
                    print(f'Preprocessing received dataset: {dataset}')
                    df = pd.DataFrame([dataset])

                    processed_data = (
                        df.pipe(remove_unnecessary_columns)
                          .pipe(remove_nan_and_duplicates)
                          .pipe(convert_features)
                    )

                    target = processed_data["target"].to_numpy().tolist()
                    features = processed_data.drop(columns=["target"]).to_dict(orient='records')

                    if not features or not target:
                        print("Empty features or target after preprocessing!")
                        continue

                    data = {"features": features, "target": target}

                    print(f"Sending processed data: {data}")
                    producer.send_message(key='1', value=json.dumps(data))
                    print(f"Successfully delivered the message: {msg}")
                except Exception as e:
                    print(f"Error during preprocessing: {e}")
        else:
            print("No messages received in preprocessing.")


if __name__ == "__main__":
        main()
