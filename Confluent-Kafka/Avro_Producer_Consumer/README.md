# Confluent Kafka Random AVRO Producer and Consumer

## Purpose
To Produce and Consume random AVRO messages to a Kafka Topic 'my_topic'.

## Prerequisites
Confluent-kafka and [avro] should be installed.

if you are using default python:
```
sudo pip install confluent-kafka
sudo pip install confluent-kafka[avro]
```
if you have multiple python (say Anaconda):
```
sudo $ANACONDA_HOME/bin/python -m pip install confluent-kafka
sudo $ANACONDA_HOME/bin/python -m pip install confluent-kafka[avro]
```

## Usage
### Producer
```
[root@apache-spark ~]$ python confluent_kakfa_random_avro_producer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

          # To Generate Continuous JSON Messages:

                Usage: python confluent_kakfa_random_avro_producer.py

          # To Generate N JSON Messages:

                  Usage: python confluent_kakfa_random_avro_producer.py 100

          # To DELETE Old Schemas:

                  /bin/curl -X DELETE http://apache-spark.hadoop.com:8081/subjects/my_topic-key;/bin/curl -X DELETE http://apache-spark.hadoop.com:8081/subjects/my_topic-value

```
### Consumer
```
[root@apache-spark ~]$ python confluent_kakfa_avro_consumer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

        Usage: python confluent_kakfa_avro_consumer.py
```

## Example
```
[root@apache-spark ~]$  python confluent_kakfa_random_avro_producer.py 2

INFO: Producing Messages

Message Produced:  {'lname': 'gaundar', 'passport_expiry_date': '2022-04-05', 'passport_make_date': '2012-04-05', 'fname': 'Timothy', 'mobile': 99869064988, 'ipaddress': '138.176.189.211', 'email': 'Timothy_gaundar@aol.com', 'principal': 'Timothy@EXAMPLE.COM'}

Message Produced:  {'lname': 'Gonzalez', 'passport_expiry_date': '2024-09-03', 'passport_make_date': '2014-09-03', 'fname': 'Donald', 'mobile': 9896454040, 'ipaddress': '112.177.240.41', 'email': 'Donald_Gonzalez@hotmail.com', 'principal': 'Donald@EXAMPLE.COM'}


[root@apache-spark ~]$  python confluent_kakfa_avro_consumer.py

INFO: Consuming Messages

Message Received:  {u'mobile': 99869064988, u'lname': u'gaundar', u'passport_expiry_date': u'2022-04-05', u'passport_make_date': u'2012-04-05', u'fname': u'Timothy', u'ipaddress': u'138.176.189.211', u'email': u'Timothy_gaundar@aol.com', u'principal': u'Timothy@EXAMPLE.COM'}

Message Received:  {u'mobile': 9896454040, u'lname': u'Gonzalez', u'passport_expiry_date': u'2024-09-03', u'passport_make_date': u'2014-09-03', u'fname': u'Donald', u'ipaddress': u'112.177.240.41', u'email': u'Donald_Gonzalez@hotmail.com', u'principal': u'Donald@EXAMPLE.COM'}
```

## Contact
nrsh13@gmail.com

