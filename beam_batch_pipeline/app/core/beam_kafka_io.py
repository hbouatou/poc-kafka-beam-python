from apache_beam import PTransform, ParDo, DoFn, Create
from kafka import KafkaConsumer, KafkaProducer


class KafkaConsume(PTransform):
    def __init__(self, bootstrap_servers, topic):
        super().__init__()
        self.consumer_conf = {'bootstrap_servers': bootstrap_servers, 'topic': topic}

    def expand(self, pcoll):
        return (
                pcoll
                | Create([self.consumer_conf])
                | ParDo(ConsumeKafkaTopic())
        )


class ConsumeKafkaTopic(DoFn):

    def process(self, consumer_conf):
        consumer = KafkaConsumer(consumer_conf['topic'], bootstrap_servers=consumer_conf['bootstrap_servers'])

        for msg in consumer:
            try:
                yield msg.key, bytes.decode(msg.value)
            except Exception as e:
                print(e)
                continue


class KafkaProduce(PTransform):

    def __init__(self, topic=None, servers='127.0.0.1:9092'):
        super().__init__()
        self._attributes = dict(
            topic=topic,
            servers=servers)

    def expand(self, pcoll):
        return (
                pcoll
                | ParDo(ProduceKafkaMessage(self._attributes))
        )


class ProduceKafkaMessage(DoFn):

    def __init__(self, attributes, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attributes = attributes

    def start_bundle(self):
        self.producer = KafkaProducer(bootstrap_servers=self.attributes["servers"])

    def finish_bundle(self):
        self.producer.close()

    def process(self, element):
        try:
            self.producer.send(self.attributes['topic'], element)
        except Exception as e:
            raise
