# Python Modules
import random, struct, sys, os, requests
import socket, json, sys, time, random
import os, random, argparse
import datetime
from dateutil.relativedelta import relativedelta
import struct, socket

# Confluent Kafka Modules
from avro import schema
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


# Print the Usage
def usage():
        print """
          # To Generate Continuous JSON Messages:

                Usage: python %s

          # To Generate N JSON Messages:

                  Usage: python %s 100

          # To DELETE Old Schemas:

                  /bin/curl -X DELETE http://%s:8081/subjects/my_topic-key;/bin/curl -X DELETE http://%s:8081/subjects/my_topic-value\n""" %(sys.argv[0].split('/')[-1],sys.argv[0].split('/')[-1],socket.gethostname(),socket.gethostname())

        sys.exit()


# Setting Compatibility to None. Helps handling Multiple Schema for a Key/Value.
def set_compatibilty_to_NONE(SCHEMA_REGISTRY_URL):
        try:
                url="{}/config".format(SCHEMA_REGISTRY_URL)
                headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json'}
                data = '{"compatibility": "NONE"}'

                print "\nINFO: Fire API Call to set Compatibility to NONE"
                response = requests.put(url, headers=headers, data=data).json()

                if response["compatibility"] == "NONE":
                        print "\nINFO: Compatibility Set to NONE Successfully"
                else:
                        print "\nINFO: Something Else is wrong. Exiting."
                        sys.exit()

        except Exception,e:
                print "\nERROR: Failed to Set Compatibility to None due to:"
                print e
                sys.exit()


# Save Scehma in SR
def save_new_value_schema_in_SR(SCHEMA_REGISTRY_URL,topic):
        try:
                subject = topic + '-value'
                url="{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject)
                headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json'}

                data = '{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"value\\",\\"namespace\\":\\"my.test\\",\\"fields\\":[{\\"name\\":\\"fname\\",\\"type\\":\\"string\\"},{\\"name\\":\\"lname\\",\\"type\\":\\"string\\"},{\\"name\\":\\"email\\",\\"type\\":\\"string\\"},{\\"name\\":\\"principal\\",\\"type\\":\\"string\\"},{\\"name\\":\\"ipaddress\\",\\"type\\":\\"string\\"},{\\"name\\":\\"mobile\\",\\"type\\":\\"long\\"},{\\"name\\":\\"passport_make_date\\",\\"type\\":[\\"string\\",\\"null\\"],\\"logicalType\\":\\"timestamp\\",\\"default\\":\\"None\\"},{\\"name\\":\\"passport_expiry_date\\",\\"type\\":\\"string\\",\\"logicalType\\":\\"date\\"}]}"}'

                print "\nINFO: Making API Call to save new schema in SR"
                response = requests.post(url, headers=headers, data=data)

                if response.ok:
                        print "\nINFO: New Schema Registered Successfully !!"
                else:
                        print "\nERROR: Failed to Register New Schema due to:"
                        print response.reason()
                        sys.exit()

        except Exception,e:
                print "\nERROR: Failed to Register New Schema due to:"
                print e
                sys.exit()


# Check if Schema for Message value Exists.
def check_topic_value_schema_existence(SCHEMA_REGISTRY_URL,topic):
        try:
                subject = topic + '-value'
                url="{}/subjects/{}".format(SCHEMA_REGISTRY_URL, subject)
                headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json'}

                data = '{"schema":"{\\"type\\":\\"record\\",\\"name\\":\\"value\\",\\"namespace\\":\\"my.test\\",\\"fields\\":[{\\"name\\":\\"fname\\",\\"type\\":\\"string\\"},{\\"name\\":\\"lname\\",\\"type\\":\\"string\\"},{\\"name\\":\\"email\\",\\"type\\":\\"string\\"},{\\"name\\":\\"principal\\",\\"type\\":\\"string\\"},{\\"name\\":\\"ipaddress\\",\\"type\\":\\"string\\"},{\\"name\\":\\"mobile\\",\\"type\\":\\"long\\"},{\\"name\\":\\"passport_make_date\\",\\"type\\":[\\"string\\",\\"null\\"],\\"logicalType\\":\\"timestamp\\",\\"default\\":\\"None\\"},{\\"name\\":\\"passport_expiry_date\\",\\"type\\":\\"string\\",\\"logicalType\\":\\"date\\"}]}"}'

                print "\nINFO: Making the API Call to SR"
                response = requests.post(url, headers=headers, data=data).json()


                if 'schema' in response.keys():
                        # Make sure you use schema.parse() which will remove the unicode.
                        # else - AttributeError: 'unicode' object has no attribute 'to_json'
                        print "\nINFO: Schema Found. Returning with Details"
                        topic_value_schema = schema.parse(response['schema'])
                else:
                        print "\nINFO: Schema Not Found. Creating Now"
                        set_compatibilty_to_NONE(SCHEMA_REGISTRY_URL)
                        print "\nINFO: Calling save_new_value_schema_in_SR()"
                        save_new_value_schema_in_SR(SCHEMA_REGISTRY_URL,topic)
                        print "\nINFO: Check Schema Existence again"
                        topic_value_schema = check_topic_value_schema_existence(SCHEMA_REGISTRY_URL,topic)

                return topic_value_schema

        except Exception,e:
                print "\nERROR: Failed to Check/Create Schema due to:"
                print e
                sys.exit()


# Save Key Schema in SR
def save_new_key_schema_in_SR(SCHEMA_REGISTRY_URL,topic):

        # Another way of creating Schema and use with Message. Earlier we created the Schema in SR Directly.
        key_schema_str = """
        {
           "namespace": "my.test",
           "name": "key",
           "type": "record",
           "fields" : [
             {
               "name" : "name",
               "type" : "string"
             }
           ]
        }
        """

        key_schema = avro.loads(key_schema_str)
        return key_schema


# Check if Schema for Message Key Exists.
def check_topic_key_schema_existence(SCHEMA_REGISTRY_URL,topic):
        try:
                # This is the second way of Getting Schema
                subject = topic + '-key'
                url="{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject),
                headers = { 'Content-Type': 'application/vnd.schemaregistry.v1+json',}

                print "\nINFO: Making the API Call to SR"
                versions_response = requests.get(
                        url="{}/subjects/{}/versions".format(SCHEMA_REGISTRY_URL, subject),
                        headers={
                                "Content-Type": "application/vnd.schemaregistry.v1+json",
                        },
                )

                latest_version = versions_response.json()[-1]
                schema_response = requests.get(
                        url="{}/subjects/{}/versions/{}".format(SCHEMA_REGISTRY_URL, subject, latest_version),
                        headers={
                                "Content-Type": "application/vnd.schemaregistry.v1+json",
                        },
                )

                key_schema_response_json = schema_response.json()

                print "\nINFO: Schema Found. Returning with Details"
                return schema.parse(key_schema_response_json["schema"])

        except Exception, e:
                print "\nWARN: Failed to get any Schema"
                print "\nINFO: Creating new by Calling save_new_key_schema_in_SR()"
                key_schema = save_new_key_schema_in_SR(SCHEMA_REGISTRY_URL,topic)
                # Here we are just preparing the schema and same will be sent with Producer mesg. If you try checking the key_schema in SR, you will fail as there will be NO ENTRIES.
                # Entry will happen when any message is written. ==> {u'message': u'Subject not found.', u'error_code': 40401}
                print "\nINFO: Schema Created. Returning with Details"
                return key_schema


# Get Acknowledgment of Delivered Message
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        #print("Message produced: {0}".format(msg.value()))
        pass


# Producer Messages to Kafka Topic
def produce_messages(avroProduce_msg, num_mesg, topic='my_topic'):
        """ Produce Messages to the Topic """

        fnames = ["James","John","Robert","Michael","William","David","Richard","Joseph","Thomas","Charles","Christopher","Daniel","Matthew","Anthony","Donald","Mark","Paul","Steven","Andrew","Kenneth","George","Joshua","Kevin","Brian","Edward","Ronald","Timothy","Jason","Jeffrey","Ryan","Gary","Jacob","Nicholas","Eric","Stephen","Jonathan","Larry","Justin","Scott","Frank","Brandon","Raymond","Gregory","Benjamin","Samuel","Patrick","Alexander","Jack","Dennis","Jerry","Tyler","Aaron","Henry","Douglas","Jose","Peter","Adam","Zachary","Nathan","Walter","Harold","Kyle","Carl","Arthur","Gerald","Roger","Keith","Jeremy","Terry","Lawrence","Sean","Christian","Albert","Joe","Ethan","Austin","Jesse","Willie","Billy","Bryan","Bruce","Jordan","Ralph","Roy","Noah","Dylan","Eugene","Wayne","Alan","Juan","Louis","Russell","Gabriel","Randy","Philip","Harry","Vincent","Bobby","Johnny","Logan","naresh","ravi","Bhanu","akash","jane","gaurav","sailesh","tom","Andrea","Steve","Kris","Virender","Jason","stephen","Daemon","Elena","manu","nimisha","Bruce","michael","Akshay"]

        lnames = ["mith","ohnson","illiams","ones","rown","avis","iller","ilson","oore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark","Rodriguez","Lewis","Lee","Walker","Hall","Allen","Young","Hernandez","King","Wright","Lopez","Hill","Scott","Green","Adams","Baker","Gonzalez","Nelson","Carter","Mitchell","Perez","Roberts","Turner","Phillips","Campbell","Parker","Evans","Edwards","Collins","Stewart","Sanchez","Morris","Rogers","Reed","Cook","Morgan","Bell","Murphy","Bailey","Rivera","Cooper","Richardson","Cox","Howard","Ward","Torres","Peterson","Gray","Ramirez","James","Watson","Brooks","Kelly","Sanders","Price","Bennett","Wood","Barnes","Ross","Henderson","Coleman","Jenkins","Perry","Powell","Long","jangra","verma","sharma","weign","nain","devgun","gaundar","dagarin","thomas","ramayna","bourne","salvator","gilbert","beniwal","kumar","khanna","khaneja","singh","bansal","gupta","kaushik"]

        emails = ["@gmail.com","@@yahoo.com","@hotmail.com","@aol.com","@hotmail.co.uk","@rediffmail.com","@ymail.com","@outlook.com","@@gmail.com","hotmail.com","@hotmail.com","@bnz.co.nz","@nbc.com","@yahoo.com","#gmail.com","@hcl.com","@#tcs.com","@tcs.com","##hcl.com"]

        try:
            print "\nINFO: Producing Messages"
            for val in xrange(0,num_mesg):
                time.sleep(1)
                fname=random.choice(fnames)
                lname=random.choice(lnames)
                email=random.choice(emails)
                ipaddress = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                mobile = random.randint(9800000000,9899999999)
                passport_expiry_date = (datetime.datetime.now() + datetime.timedelta(random.randint(1,100)*365/12))
                passport_make_date = (passport_expiry_date - relativedelta(years=10))

                passport_make_date = passport_make_date.date()
                passport_expiry_date = passport_expiry_date.date()


                if val%5 == 0:
                        mobile = "9"+str(mobile)

                key =  {"name": "Key"}

                json_data={"fname" : fname, "lname" : lname, "email" : fname+"_"+lname+email, "principal" : fname+"@EXAMPLE.COM", "passport_make_date" : str(passport_make_date), "passport_expiry_date" : str(passport_expiry_date), "ipaddress" : ipaddress , "mobile" : int(mobile)}

                print "\nMessage Produced: " ,json_data

                avroProduce_msg.produce(topic=topic, value=json_data, key=key, callback=acked )
                # Polls the producer for events and calls the corresponding callbacks (if registered).
                avroProduce_msg.poll(0.5)

            avroProduce_msg.flush()

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

        schemaRegistryServer = hostname
        schemaRegistryPort = 8081

        topic = 'my_topic'

        print """\nINFO: Kakfa Connection Details:

        Kafka Broker : %s
        Zookeeper    : %s
        Topic        : %s """ %(kafkaBroker,zookeeper,topic)

        SCHEMA_REGISTRY_URL = 'http://'+schemaRegistryServer+':'+str(schemaRegistryPort)

        print "\nINFO: Check Schema Existence for VALUE using check_topic_value_schema_existence() Function"
        topic_value_schema = check_topic_value_schema_existence(SCHEMA_REGISTRY_URL,topic)

        print "\nINFO: Check Schema Existence for KEY using check_topic_key_schema_existence() Function"
        topic_key_schema = check_topic_key_schema_existence(SCHEMA_REGISTRY_URL,topic)

        avroProduce_msg = AvroProducer({
            'bootstrap.servers': kafkaBroker,
            'schema.registry.url': SCHEMA_REGISTRY_URL
            }, default_key_schema=topic_key_schema, default_value_schema=topic_value_schema)


        print "\nINFO: All SET for Producing Messages"

        produce_messages(avroProduce_msg,int(num_mesg),topic=topic)
