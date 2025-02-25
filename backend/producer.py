from confluent_kafka import Producer


class KafkaProducer:
    def __init__(self, bootstrap_servers, topic):
        self.producer = Producer(
            {
                'bootstrap.servers': bootstrap_servers,
                'default.topic.config': {'api.version.request': True},
                'security.protocol': 'PLAINTEXT'
            }
        )
        self.topic = topic

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Error sending the message: {err}")
        else:
            print(f"Successfully delivered the message: {msg}")

    def send_message(self, key, value):
        try:
            print(f"Sending message: {value}")
            self.producer.produce(self.topic, key=key, value=value, callback=self.delivery_report)
            self.producer.poll(1)
        except Exception as e:
            print(f"Failed to send message: {e}")

    def close(self):
        self.producer.flush()
