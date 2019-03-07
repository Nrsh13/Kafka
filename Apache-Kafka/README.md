# Pykafka Random JSON Producer and Consumer

## Purpose
To Produce and Consume random JSON messages to a Kafka Topic 'mytopic'.

## Prerequisites
pykafka should be installed.
To install pykafka:

if you are using default python:
```
sudo pip install pykafka
```
if you have multiple python (say Anaconda):
```
sudo $ANACONDA_HOME/bin/python -m pip install pykafka
```

## Usage
### Producer
```
[root@apache-spark ~]$ python pykafka_random_json_producer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

          # To Generate Continuous JSON Messages:

                Usage: python pykafka_random_json_producer.py

          # To Generate N JSON Messages:

                Usage: python pykafka_random_json_producer.py 100
```
### Consumer
```
[root@apache-spark ~]$ python pykafka_json_consumer.py help

        ALERT: This Script assumes All Services are running on same Machine apache-spark.hadoop.com !!
        if NOT, Update the required Details in Main() section of the Script.

        Usage: python pykafka_json_consumer.py
```

## Example
```
[root@apache-spark ~]$  python pykafka_random_json_producer.py 2

INFO: Producing Messages

Message Produced: {"fname" : "Edward","lname" : "Powell","email" : "Edward_Powellhotmail.com","principal" : "Edward@EXAMPLE.COM","passport_make_date" : "2012-09-04 15:23:43.212029","passport_expiry_date" : "2022-09-04 15:23:43.212029","ipaddress" : "122.138.187.177" , "mobile" : "99801949021"}

Message Produced: {"fname" : "Anthony","lname" : "bourne","email" : "Anthony_bourne@hotmail.com","principal" : "Anthony@EXAMPLE.COM","passport_make_date" : "2010-02-04 15:23:44.220641","passport_expiry_date" : "2020-02-04 15:23:44.220641","ipaddress" : "147.92.131.163" , "mobile" : "9877518643"}


[root@apache-spark ~]$  python pykafka_json_consumer.py

INFO: Consuming Messages

Message Received: {"fname" : "Edward","lname" : "Powell","email" : "Edward_Powellhotmail.com","principal" : "Edward@EXAMPLE.COM","passport_make_date" : "2012-09-04 15:23:43.212029","passport_expiry_date" : "2022-09-04 15:23:43.212029","ipaddress" : "122.138.187.177" , "mobile" : "99801949021"}

Message Received: {"fname" : "Anthony","lname" : "bourne","email" : "Anthony_bourne@hotmail.com","principal" : "Anthony@EXAMPLE.COM","passport_make_date" : "2010-02-04 15:23:44.220641","passport_expiry_date" : "2020-02-04 15:23:44.220641","ipaddress" : "147.92.131.163" , "mobile" : "9877518643"}
```

## Contact
nrsh13@gmail.com
