from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from confluent_kafka import Producer

import json

v = {
    "sender_uid":123123,
    'sender_bid':2,
    'devid':'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
    'os_type': "DeviceOs_Unknow",
    'platform': "DevicePlatform_Unknow",
    'receiver_type': "kSingleRecver",
    'receiver_bid': 10,
    'receiver_uid': 2,
    'send_time': 1496299410,
    'message_type': "kTextChatMsg",
    'body': 'asdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdf',
}


value_schema = avro.load('im.avsc')

avroProducer = AvroProducer({'bootstrap.servers': 'da100:9092', 'schema.registry.url': 'http://da100:8083'}, default_value_schema=value_schema)
import time
while True:
    time.sleep(2)
    avroProducer.produce(topic='t_im_message', value=v)
