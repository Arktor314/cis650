"""
CIS 650
SPRING 2016
usage: pass_token_mqtt.py <UID> <upstream UID>

> For python mosquitto client $ sudo pip install paho-mqtt
> Command line arg to check status of broker $ /etc/init.d/mosquitto status

Adapted from provided pass_token_mqtt_v2.py file.
"""
import sys
import time
import paho.mqtt.client as mqtt
from math import ceil

#############################################
## Get UID and upstream_UID from args
#############################################

if len(sys.argv) != 3:
	print('ERROR\nusage: pass_token_mqtt.py <int: UID> <int: upstream UID>')
	sys.exit()

try:
    UID = int(sys.argv[1])
    upstream_UID = int(sys.argv[2])
except ValueError:
	print('ERROR\nusage: pass_token_mqtt.py <int: UID > <int: upstream UID >')
	sys.exit()

#####
# My Additions
#####
isActive = True # Initialize as active
isLeader = False    # Initialize as not the leader
isWorking = False   # Initialize as not working


def is_prime(n):
    if n < 2:
        return False
    for i in range (2,int(ceil(n**0.5))):
        if n%i == 0:
            return False
    return True

def count_primes(start,pair):
    lower = pair[0]
    upper = pair[1]
    count = 0
    for i in range(lower,upper):
        if is_prime(i):
            count += 1
    return count + start

def is_leader_message(msg):
    if msg[:1] == 'L':  # Leader message starts with L
        return True
    return False

def make_leader_message(uid):
    return "L" + str(uid)

def get_leader_value(msg):
    if msg[:1] != 'L':
        print("ERROR: This is not a leader message. " + msg)
    return int(msg[1:])

def make_count_message(startValue, countSoFar):
    return str(startValue) + "_" + str(countSoFar)

def read_count_message(msg):
    index = msg.index("_")  # Will throw exception if not found. This is probably ok?
    startValue = msg[:index]
    count = msg[index+1:]
    print("" + str(time.time()) + ": received count of " + count + " and start value of " + startValue)
    return (int(startValue),int(count))


#############################################
## MQTT settings
#############################################

broker      = 'green0'
port        = 1883

# publish topics
send_token_topic = 'token/'+str(upstream_UID)

# subscribe topics
token_topic = 'token/'+str(UID)
will_topic = 'will/'

#quality of service
qos = 0

##############################################
## MQTT callbacks
##############################################

#Called when the broker responds to our connection request
def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("Connection failed. RC: " + str(rc))
    else:
        print("Connected successfully with result code RC: " + str(rc))

#Called when a published message has completed transmission to the broker
def on_publish(client, userdata, mid):
    print("Message ID "+str(mid)+ " successfully published")

#Called when message received on token_topic
def on_token(client, userdata, msg):
    print("Received message: "+str(msg.payload)+". On topic: "+msg.topic)
    global isActive
    global isLeader
    global isWorking
    if (isActive and not isLeader and not isWorking):
        if (not is_leader_message(msg.payload)):
            if int(msg.payload) > UID:
                client.publish(send_token_topic, msg.payload)
                isActive = False
            elif int(msg.payload) == UID:
                isActive = False
                isLeader = True
                client.publish(send_token_topic, make_leader_message(UID))
        # If ours is greater, do nothing.
        else:
            client.publish(send_token_topic, msg.payload)
    elif (not isActive and not isWorking):
        if is_leader_message(msg.payload):
            isWorking = True
            if (get_leader_value(msg.payload) == UID):
                isLeader = True
                client.publish(send_token_topic, make_count_message(3, 1))
            else:
                client.publish(send_token_topic, msg.payload)
        else:
            client.publish(send_token_topic, msg.payload)
    elif (isWorking):
        pair = read_count_message(msg.payload)
        startValue = pair[0]
        count = pair[1]
        endValue = startValue + 100001
        newCount = count_primes(count,(startValue,endValue))    #endValue isn't included in the count.
        client.publish(send_token_topic, make_count_message(endValue,newCount))

#Called when message received on will_topic
def on_will(client, userdata, msg):
    print("Received message: "+str(msg.payload)+"on topic: "+msg.topic)

#Called when a message has been received on a subscribed topic (unfiltered)
def on_message(client, userdata, msg):
    print("Received message: "+str(msg.payload)+"on topic: "+msg.topic)
    print('unfiltered message')


#############################################
## Connect to broker and subscribe to topics
#############################################
try:
    # create a client instance
    client = mqtt.Client(str(UID))

    # setup will for client
    will_message = "Dead UID: {}, upstream_UID: {} ".format(UID,upstream_UID)
    client.will_set(will_topic, will_message)

    # callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_message = on_message

    # callbacks for specific topics
    client.message_callback_add(token_topic, on_token)
    client.message_callback_add(will_topic, on_will)

    # connect to broker
    client.connect(broker, port, keepalive=30)

    # subscribe to list of topics
    client.subscribe([(token_topic, qos),
                      (will_topic, qos),
                      ])
    time.sleep(2)
    # initiate pub/sub
#    if UID == 1:
#        time.sleep(5)
#        client.publish(send_token_topic, UID)

    if (isActive):
        client.publish(send_token_topic, UID)

    # network loop
    client.loop_forever()

except (KeyboardInterrupt):
    print("Interrupt received")
except (RuntimeError):
    print("Runtime Error")
    client.disconnect()
