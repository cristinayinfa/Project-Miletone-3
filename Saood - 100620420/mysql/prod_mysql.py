from kafka import KafkaProducer;
import json;
import time
import io;
from avro.io import DatumWriter, BinaryEncoder
import avro.schema
import csv
import os
script_dir = os.path.dirname(__file__)
khv_path = "D:\Year 4\Cloud Computing\Milestone 3\SOFE4630U-tut3\data\kvh.csv"
groundtruth_path = "D:\Year 4\Cloud Computing\Milestone 3\SOFE4630U-tut3\data\groundtruth.csv"

schemaID=100007;

data=json.load(open('cred.json'))
bootstrap_servers=data['bootstrap_servers'];
sasl_plain_username=data['Api key'];
sasl_plain_password=data['Api secret'];

schema = avro.schema.parse(open("./schema2.avsc").read())
writer = DatumWriter(schema)

def encode(value):
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return schemaID.to_bytes(5, 'big')+bytes_writer.getvalue()

value1 ={"sen1": "", "sen2": ""}
value2 ={"groundtruth1": "","groundtruth2": "","groundtruth3": "","groundtruth4": "","groundtruth5": "","groundtruth6": "","groundtruth7": ""}


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,security_protocol='SASL_SSL',sasl_mechanism='PLAIN',\
    sasl_plain_username=sasl_plain_username,sasl_plain_password=sasl_plain_password,value_serializer=lambda m: encode(m))

with open(khv_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        line_count+=1
        if line_count >=10:
            break
        value1["sen1"] = row[0]
        value1["sen2"] = row[1]
        producer.send('ToMySQLNew', value1);
with open(groundtruth_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        line_count+=1
        if line_count >=10:
            break
        value2["groundtruth1"] = row[0]
        value2["groundtruth2"] = row[1]
        value2["groundtruth3"] = row[2]
        value2["groundtruth4"] = row[3]
        value2["groundtruth5"] = row[4]
        value2["groundtruth6"] = row[5]
        value2["groundtruth7"] = row[6]
        producer.send('ToMySQLNew', value2);

producer.close();
