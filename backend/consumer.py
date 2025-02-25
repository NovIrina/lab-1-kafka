from confluent_kafka import Consumer, KafkaError

class KafkaConsumer:
    def __init__(self, config):
        self.consumer = Consumer(config)

    def subscribe(self, topics):
        if isinstance(topics, str):  # If a single topic is passed as a string
            topics = [topics]  # Convert to a list
        self.topic = topics  # Store the topics if needed
        self.consumer.subscribe(topics)

    def poll(self, timeout=1.0):
        return self.consumer.poll(timeout)

    def consume(self, num_messages=1, timeout=1.0):
        messages = []
        for _ in range(num_messages):
            msg = self.poll(timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    self.error_handler(msg.error())
                    continue
            messages.append(msg)
        return messages

    def commit(self, offsets=None):
        self.consumer.commit(offsets)

    def close(self):
        self.consumer.close()

    def assign(self, partitions):
        self.consumer.assign(partitions)

    def seek(self, partition, offset):
        self.consumer.seek(partition, offset)

    def get_metadata(self):
        return self.consumer.list_topics()

    def error_handler(self, error):
        print(f"Error: {error}")
