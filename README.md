# Pykafka_Random_JSON_Producer

## Purpose
To Produce random JSON messages to a Kafka Topic.

## Prerequisites
pykafka should be installed.

To install pykafka:
  if you are using default python:
      pip install pykafka
  if you have multiple python (say Anaconda):
      sudo $ANACONDA_HOME/bin/python -m pip install pykafka

```
[root@apache-spark ~]# pip install pykafka
Collecting pykafka
Requirement already satisfied: kazoo in /usr/local/anaconda2/lib/python2.7/site-packages (from pykafka)
Requirement already satisfied: six>=1.5 in /usr/local/anaconda2/lib/python2.7/site-packages (from pykafka)
Requirement already satisfied: tabulate in /usr/local/anaconda2/lib/python2.7/site-packages (from pykafka)
Installing collected packages: pykafka
Successfully installed pykafka-2.7.0

[root@apache-spark ~]# python
Python 2.7.14 |Anaconda custom (64-bit)| (default, Mar 12 2018, 12:37:12)
[GCC 7.2.0] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>> import pykafka
>>> exit()
```

## Usage
```
[root@apache-spark ~]$ python random_json_pykafka_producer.py -h

usage: random_json_pykafka_producer.py [-h] [-s SERVER] [-p PORT] [-t TOPIC]

optional arguments:
  -h, --help            show this help message and exit
  -s SERVER, --server SERVER
                        Kafka Broker Node
  -p PORT, --port PORT  Kafka Brokers Port
  -t TOPIC, --topic TOPIC
                        Topic Name
```

## Example
```
[root@apache-spark ~]$ python random_json_kafka_producer.py -s apache-kafka.abc.com -p 9092 -t test

Producing JSON messages to:
    Kafka Broker: apache-kafka.abc.com:9092
    Topic: test

Message Sent -
{"fname" : "Roy","lname" : "Bell","email" : "Roy_Bell##hcl.com","principal" : "Roy@EXAMPLE.COM","passport_make_date" : "2008-10-24 12:10:27.556631","passport_expiry_date" : "2018-10-24 12:10:27.556631","ipaddress" : "104.162.118.232" , "mobile" : "9856486005"}
Message Sent -
{"fname" : "Jerry","lname" : "Robinson","email" : "Jerry_Robinson##hcl.com","principal" : "Jerry@EXAMPLE.COM","passport_make_date" : "2011-06-23 12:10:28.566032","passport_expiry_date" : "2021-06-23 12:10:28.566032","ipaddress" : "242.214.220.135" , "mobile" : "9876904872"}
Message Sent -
{"fname" : "Adam","lname" : "Rivera","email" : "Adam_Rivera@nbc.com","principal" : "Adam@EXAMPLE.COM","passport_make_date" : "2011-03-24 12:10:29.571494","passport_expiry_date" : "2021-03-24 12:10:29.571494","ipaddress" : "9.201.144.77" , "mobile" : "9860416979"}
Message Sent -
{"fname" : "Jason","lname" : "Robinson","email" : "Jason_Robinson#gmail.com","principal" : "Jason@EXAMPLE.COM","passport_make_date" : "2014-02-21 12:10:30.581339","passport_expiry_date" : "2024-02-21 12:10:30.581339","ipaddress" : "126.175.78.218" , "mobile" : "9861991883"}
press CTRL+z to STOP
```

Check these messages in Kafka:
```
[root@apache-spark ~]$ kafka-console-consumer --bootstrap-server apache-kafka.abc.com:9092 --topic test
{"fname" : "Roy","lname" : "Bell","email" : "Roy_Bell##hcl.com","principal" : "Roy@EXAMPLE.COM","passport_make_date" : "2008-10-24 12:10:27.556631","passport_expiry_date" : "2018-10-24 12:10:27.556631","ipaddress" : "104.162.118.232" , "mobile" : "9856486005"}
{"fname" : "Jerry","lname" : "Robinson","email" : "Jerry_Robinson##hcl.com","principal" : "Jerry@EXAMPLE.COM","passport_make_date" : "2011-06-23 12:10:28.566032","passport_expiry_date" : "2021-06-23 12:10:28.566032","ipaddress" : "242.214.220.135" , "mobile" : "9876904872"}
{"fname" : "Adam","lname" : "Rivera","email" : "Adam_Rivera@nbc.com","principal" : "Adam@EXAMPLE.COM","passport_make_date" : "2011-03-24 12:10:29.571494","passport_expiry_date" : "2021-03-24 12:10:29.571494","ipaddress" : "9.201.144.77" , "mobile" : "9860416979"}
{"fname" : "Jason","lname" : "Robinson","email" : "Jason_Robinson#gmail.com","principal" : "Jason@EXAMPLE.COM","passport_make_date" : "2014-02-21 12:10:30.581339","passport_expiry_date" : "2024-02-21 12:10:30.581339","ipaddress" : "126.175.78.218" , "mobile" : "9861991883"}
^CProcessed a total of 4 messages
```

## Contact
nrsh13@gmail.com
