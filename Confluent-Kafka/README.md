# Confluent Kafka Random JSON Producer and Consumer

## Purpose
To Produce and Consume random JSON messages to a Kafka Topic 'mytopic'.

## Prerequisites
To install confluent-kafka:

if you are using default python:
```
sudo pip install confluent-kafka
```
if you have multiple python (say Anaconda):
```
sudo $ANACONDA_HOME/bin/python -m pip install confluent-kafka
```

## Usage
### Producer
```
[root@apache-spark ~]$ python confluent_kakfa_random_json_producer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

          # To Generate Continuous JSON Messages:

                Usage: python confluent_kakfa_random_json_producer.py

          # To Generate N JSON Messages:

                Usage: python confluent_kakfa_random_json_producer.py 100
```
### Consumer
```
[root@apache-spark ~]$ python confluent_kakfa_json_consumer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

        Usage: python confluent_kakfa_json_consumer.py
```

## Example
```
[root@apache-spark ~]$  python confluent_kakfa_random_json_producer.py 2

INFO: Producing Messages

Message Produced: {"fname" : "Harry","lname" : "Edwards","email" : "Harry_Edwards@gmail.com","principal" : "Harry@EXAMPLE.COM","passport_make_date" : "2010-04-05 15:40:52.981742","passport_expiry_date" : "2020-04-05 15:40:52.981742","ipaddress" : "233.172.217.11" , "mobile" : "99870841981"}

Message Produced: {"fname" : "Peter","lname" : "Nelson","email" : "Peter_Nelson@hotmail.com","principal" : "Peter@EXAMPLE.COM","passport_make_date" : "2013-09-04 15:40:54.483212","passport_expiry_date" : "2023-09-04 15:40:54.483212","ipaddress" : "233.43.75.188" , "mobile" : "9827272574"}


[root@apache-spark ~]$  python confluent_kakfa_json_consumer.py

INFO: Consuming Messages

Message Received: {"fname" : "Harry","lname" : "Edwards","email" : "Harry_Edwards@gmail.com","principal" : "Harry@EXAMPLE.COM","passport_make_date" : "2010-04-05 15:40:52.981742","passport_expiry_date" : "2020-04-05 15:40:52.981742","ipaddress" : "233.172.217.11" , "mobile" : "99870841981"}

Message Received: {"fname" : "Peter","lname" : "Nelson","email" : "Peter_Nelson@hotmail.com","principal" : "Peter@EXAMPLE.COM","passport_make_date" : "2013-09-04 15:40:54.483212","passport_expiry_date" : "2023-09-04 15:40:54.483212","ipaddress" : "233.43.75.188" , "mobile" : "9827272574"}
```

## Contact
nrsh13@gmail.com
