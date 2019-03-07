# Python Modules
import sys, os, json, time

# Confluent Kafka Modules
from confluent_kafka import Consumer, KafkaError


# Print the Usage
def usage():
        print """
        Usage: python %s
        """ %(sys.argv[0].split('/')[-1])

        sys.exit()


# Consume Messages from Kafka Topic
def consume_messages(client, topic="mytopic"):

        print "\nINFO: Consuming Messages \n"

        client.subscribe([topic])

        while True:
            try:
                msg = client.poll(1.0)
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

            print('Message Received: {}\n'.format(msg.value()))

        client.close()


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

        topic = 'mytopic'

        print """\nINFO: Kakfa Connection Details:

        Kafka Broker : %s
        Zookeeper    : %s
        Topic        : %s  """ %(kafkaBroker,zookeeper,topic)

        conf = {
            'bootstrap.servers': kafkaBroker,
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        }

        print "\nINFO: Create Client obj for Kafka Connection"
        client = Consumer(**conf)

        consume_messages(client, topic=topic)
