import pandas as pd
import json
import random
import time

from producer import KafkaProducer

def collect_data(dataset, producer, sleep_duration=None):
    for _, record in dataset.iterrows():
        # Convert the Series to a dictionary before serializing to JSON
        record_dict = record.to_dict()
        producer.send_message(key='1', value=json.dumps(record_dict))
        if sleep_duration:
            time.sleep(sleep_duration)
        else: 
            time_to_sleep = random.uniform(1, 5)
            time.sleep(time_to_sleep)
    producer.close()

def main():
    print("Collecting data...")
    dataset = pd.read_csv("data/test_data.csv")
    producer_1 = KafkaProducer(bootstrap_servers='localhost:9095', topic='raw_data')

    collect_data(dataset, producer_1, 3)

if __name__ == "__main__":
    main()
