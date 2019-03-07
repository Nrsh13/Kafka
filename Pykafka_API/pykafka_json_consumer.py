# Python Modules
import sys, os, json, time

# Confluent Kafka Modules
from pykafka import KafkaClient


# Print the Usage
def usage():
        print """
        Usage: python %s
        """ %(sys.argv[0].split('/')[-1])

        sys.exit()


# Consume Messages from Kafka Topic
def consume_messages(client, topic="mytopic"):

        print "\nINFO: Consuming Messages \n"

        topic = client.topics[topic]

        consumer = topic.get_simple_consumer()

        # Get the Last Message offset so that we can only read the new coming messages.
        lastest_offset = topic.latest_available_offsets()[0][0][0]

        try:
            for msg in consumer:
                if msg.offset >= int(lastest_offset) :
                    print('Message Received: {}\n'.format(msg.value))
        except Exception, e:
            print "\nExiting with Error: \n", e


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

        print "\nINFO: Create Client obj for Kafka Connection"
        client = KafkaClient(kafkaBroker)

        consume_messages(client, topic=topic)
