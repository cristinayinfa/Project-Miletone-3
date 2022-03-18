from kafka import KafkaProducer
import json
import time
import io
import os
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import keyring
import numpy as np

data = json.load(open('cred.json'))
bootstrap_servers = data['bootstrap_servers']
sasl_plain_username = data['Api key']
sasl_plain_password = data['Api secret']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, security_protocol='SASL_SSL', sasl_mechanism='PLAIN',
                         sasl_plain_username=sasl_plain_username, sasl_plain_password=sasl_plain_password, key_serializer=lambda v: v.encode())

for filen in os.scandir('./pics'):
    count = 0
    if filen.is_file():
        with open(filen.path, 'rb') as f:

            value = f.read()

            key = 'row' + str(count)
            count = count + 1
            producer.send('ToRedis', value, key=key)
producer.close()
