# Python Modules
import random, struct, sys, os, requests
import socket, json, sys, time, random
import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket

# Pykafka Modules
from pykafka import KafkaClient


# Print the Usage
def usage():
        print """
          # To Generate Continuous JSON Messages:

                Usage: python %s

          # To Generate N JSON Messages:

                Usage: python %s 100
        """ %(sys.argv[0].split('/')[-1],sys.argv[0].split('/')[-1])
        sys.exit()


# Producer Messages to Kafka Topic
def produce_messages(topic,num_mesg):
        """ Produce Messages to the Topic """

        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]


        print "\nINFO: Producing %s Messages sys.maxint\n" %num_mesg

        # Call get_sync_producer() function for topic
        with topic.get_sync_producer() as producer:

          try:
            for val in xrange(0,int(num_mesg)):

                fname=random.choice(fnames)
                lname=random.choice(lnames)
                email=random.choice(emails)
                ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                mobile = random.randint(9800000000,9899999999)
                passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
                passport_make_date = (passport_expiry_date - relativedelta(years=10))

                if val%5 == 0:
                        mobile = "9"+str(mobile)

                message='{"fname" : "%s","lname" : "%s","email" : "%s_%s%s","principal" : "%s@EXAMPLE.COM","passport_make_date" : "%s","passport_expiry_date" : "%s","ipaddress" : "%s" , "mobile" : "%s"}' %(fname,lname, fname, lname,email, fname, passport_make_date, passport_expiry_date, ipaddress, mobile)

                producer.produce(message,partition_key='0')

                print "Message Produced:" , message , "\n"
                time.sleep(1)
                #produce_messages(topic)

          except KeyboardInterrupt:
            pass


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
                num_mesg = 10
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
        Topic        : %s  """ %(kafkaBroker,zookeeper,topic)

        print "\nINFO: Topic 'mytopic' will be Created automatically If Does not Exist"

        print "\nINFO: Create Client obj for Kafka Connection"

        # Create a Client Connection
        client = KafkaClient(hosts=kafkaBrokerServer+":"+str(kafkaBrokerPort))

        # List the Topics
        topic = client.topics[topic]

        produce_messages(topic,num_mesg)
