# Python Modules
import socket, json, sys, time, random
import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket

# Confluent Kafka Modules
from confluent_kafka import Producer
from confluent_kafka import admin
#from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource, ConfigEntry
from confluent_kafka import KafkaException


# Print the Usage
def usage():
        print """
          # To Generate Continuous JSON Messages:

                Usage: python %s

          # To Generate N JSON Messages:

                Usage: python %s 100
          \n""" %(sys.argv[0].split('/')[-1],sys.argv[0].split('/')[-1])

        sys.exit()


# Get Acknowledgment of Delivered Message
def acked(err, msg):
    if err is not None:
        print("Failed to Deliver Message: {0}: {1}\n"
              .format(msg.value(), err.str()))
    else:
        print("\nMessage Produced: {0} ".format(msg.value()))



# Check and Create if the Topic Does not Exist.
def check_topic_existence(connection, topic):
        """ Check Topic Existence and Create it if needed"""

        chk_topic = connection.list_topics().topics

        if chk_topic.get(topic):
                print "\nINFO: %s topic Exists." %topic
        else:
                print "\nINFO: Creating the Topic %s" %topic
                # NewTopic specifies per-topic settings for passing to passed to AdminClient.create_topics().
                setTopic = admin.NewTopic(topic ,num_partitions=1, replication_factor=1)
                fs = connection.create_topics([setTopic],request_timeout=10)

                for t,f in fs.items():
                    try:
                        f.result()  # The result itself is None
                        print("Topic {} Created".format(t))
                    except KafkaException as e:
                        print("Falied to Create Topic {}: {}".format(t, e))
                        sys.exit()


# Producer Messages to Kafka Topic
def produce_messages(client, num_mesg, topic):
        """ Produce Messages to the Topic """

        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

        try:
            print "\nINFO: Producing Messages"
            for val in xrange(0,num_mesg):
                fname=random.choice(fnames)
                lname=random.choice(lnames)
                email=random.choice(emails)
                ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                mobile = random.randint(9800000000,9899999999)
                passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
                passport_make_date = (passport_expiry_date - relativedelta(years=10))

                if val%5 == 0:
                        mobile = "9"+str(mobile)

                json_data='{"fname" : "%s","lname" : "%s","email" : "%s_%s%s","principal" : "%s@EXAMPLE.COM","passport_make_date" : "%s","passport_expiry_date" : "%s","ipaddress" : "%s" , "mobile" : "%s"}' %(fname,lname, fname, lname,email, fname, passport_make_date, passport_expiry_date, ipaddress, mobile)

                client.produce(topic , value = json_data, callback=acked)

                # Polls the producer for events and calls the corresponding callbacks (if registered).
                client.poll(0.5)
                time.sleep(1)

        except KeyboardInterrupt:
            pass

        # Wait for all messages in the Producer queue to be delivered. This is a convenience method that calls poll() until len() is zero or the optional timeout elapses.
        client.flush(30)


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
        elif len(sys.argv) == 2 and not sys.argv[1].isdigit():
                usage()

        if len(sys.argv) != 2:
                num_mesg = sys.maxint
        else:
                num_mesg = sys.argv[1]


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
        Topic        : %s """ %(kafkaBroker,zookeeper,topic)

        print "\nINFO: Creating connection obj for Admin Task"
        connection = admin.AdminClient({'bootstrap.servers':kafkaBroker})

        print "\nINFO: Check if Topic %s exists. Else Create it" %topic
        check_topic_existence(connection, topic)

        conf = {'bootstrap.servers': kafkaBroker}

        print "\nINFO: Create Client obj for Kafka Connection"
        client = Producer(**conf)

        produce_messages(client,int(num_mesg),topic)
