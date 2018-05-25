import json, sys, time, random
from pykafka import KafkaClient
from dateutil.relativedelta import relativedelta
import struct, socket, datetime, argparse

# This Function will generate the messages -
def produce_msgs(topic):


        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

        fname=random.choice(fnames)
        lname=random.choice(lnames)
        email=random.choice(emails)
        ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
        mobile = random.randint(9800000000,9899999999)
        passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
        passport_make_date = (passport_expiry_date - relativedelta(years=10))


        # Call get_sync_producer() function for topic
        with topic.get_sync_producer() as producer:

                message='{"fname" : "%s","lname" : "%s","email" : "%s_%s%s","principal" : "%s@EXAMPLE.COM","passport_make_date" : "%s","passport_expiry_date" : "%s","ipaddress" : "%s" , "mobile" : "%s"}' %(fname,lname, fname, lname,email, fname, passport_make_date, passport_expiry_date, ipaddress, mobile)

                producer.produce(message,partition_key='0')

                print "Message Sent - \n" , message

                time.sleep(1)
                produce_msgs(topic)

if __name__ == '__main__':

        global client
        global topic

        print " PROJECT: \t Random Kafka JSON Message Producer \n AUTHOR: \t Naresh Jangra \n CREATED: \t 25th May, 2018 \n PURPOSE: \t To produce a JSON message to a Kafka Topic!!"
        print "\n"
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--server", help="Kafka Broker Node")
        parser.add_argument("-p", "--port", default="9092", help="Kafka Brokers Port", type=int)
        parser.add_argument("-t" , "--topic",help="Topic Name")

        args = parser.parse_args()

        if (args.server == None or args.topic == None):
                parser.print_help()
                sys.exit()
        else:
                print "Producing JSON messages to:"
                print "    Kafka Broker: %s" %(args.server+":"+str(args.port))
                print "    Topic: %s" %(args.topic)
                print "\n"

        # Create a Client Connection
        client = KafkaClient(hosts=args.server+":"+str(args.port))
        #client = KafkaClient("drlkfb01:9092")
        # List the Topics
        #print "\nList of Topics - ", client.topics

        # Choose a Topic you want to Consume
        #input = raw_input("\nChoose the Topic from List - ")

        # List the Topics
        topic = client.topics[args.topic]

        produce_msgs(topic)
