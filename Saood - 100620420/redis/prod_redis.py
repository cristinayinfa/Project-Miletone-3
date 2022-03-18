from kafka import KafkaProducer;
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import numpy as np;
import os

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers'];
sasl_plain_username=data['Api key'];
sasl_plain_password=data['Api secret'];
 

        
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,key_serializer=lambda v: v.encode())

for filen in os.scandir("D:\Year 4\Cloud Computing\Milestone 3\SOFE4630U-tut3\data\images"):
    if filen.is_file():
        with open(filen.path,"rb") as f:
            value = f.read()
            producer.send('ToRedis', value,key='RandImages');

producer.close();
