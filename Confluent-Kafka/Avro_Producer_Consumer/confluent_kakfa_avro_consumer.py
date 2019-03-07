# Python Modules
import sys, os, json, time

# Confluent Kafka Modules
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


# Print the Usage
def usage():
        print """
        Usage: python %s
        """ %(sys.argv[0].split('/')[-1])

        sys.exit()


# Consume Messages from Kafka Topic
def consume_messages(avroConsume_client, topic="my_topic"):

        print "\nINFO: Consuming Messages "

        avroConsume_client.subscribe([topic])

        while True:
            try:
                msg = avroConsume_client.poll(10)
            except SerializerError as e:
                print("Message deserialization failed for {}: {}".format(msg, e))
                break

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Apart from msg.values, we have: msg.headers(), msg.offset(), msg.partition(), msg.key()
            # Below we are setting some Default Values in case a Key Does not exists. The same will be handled by Avro when we use Java due to Java's SpecificRecord Class.

            print "\nMessage Received: " , msg.value()

            # You can Choose Required Fields
            #print "\nChoosing Desired Fields: ", msg.value()["fname"] , msg.value()["lname"] , msg.value().get("passport_make_date","None"), msg.value().get("passport_expiry_date", "None")

        avroConsume_client.close()


# Main Function
if __name__ == '__main__':

        import socket
        hostname = socket.gethostname()

        print """
        ALERT: This Script assumes All Services are running on same Machine %s !!
        if NOT, Update the required Details in Main() section of the Script.""" %hostname

        # Check Number of Arguments
        if len(sys.argv) == 2 and sys.argv[1].lower() in ['h', 'help', 'usage']:
                usage()

        # Kafka Details
        kafkaBrokerServer = hostname
        kafkaBrokerPort = 9092
        kafkaBroker = kafkaBrokerServer+":"+str(kafkaBrokerPort)

        zookeeperServer = hostname
        zookeeperPort = 2185
        zookeeper = zookeeperServer+":"+str(zookeeperPort)

        schemaRegistryServer = hostname
        schemaRegistryPort = 8081

        topic = 'my_topic'

        SCHEMA_REGISTRY_URL = 'http://'+schemaRegistryServer+':'+str(schemaRegistryPort)

        print """\nINFO: Kakfa Connection Details:

        Kafka Broker : %s
        Zookeeper    : %s
        Topic        : %s  """ %(kafkaBroker,zookeeper,topic)

        conf = {
            'bootstrap.servers': kafkaBroker,
            'group.id': 'mygroup',
            'schema.registry.url': SCHEMA_REGISTRY_URL
        }

        print "\nINFO: Create Client obj for Kafka Connection"

        avroConsume_client = AvroConsumer(conf)

        consume_messages(avroConsume_client, topic=topic)
