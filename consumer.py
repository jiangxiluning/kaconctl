from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


c = AvroConsumer({'bootstrap.servers': 'da100:9092', 'group.id': 'groupid11', 'schema.registry.url': 'http://da100:8083'})

c.subscribe(['community_t_user'])

running = True
while running:
    try:
        msg = c.poll()
        if msg:
            if not msg.error():
                print(msg.value())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False

c.close()
