import time
from google.cloud import pubsub_v1
import json
import os

############# Edit this section ################
project_id = "[YOUR_TOPIC]"
topic_id = "my-topic"
path_to_file = './[YOUR_FILE_PATH].json'
num_msg_to_send = 200
time_to_send = 3600 #in seconds
################################################


wait_time = time_to_send/num_msg_to_send

with open(path_to_file) as f:
  f = json.load(f)

def pick_random_line(data):
    import random
    line_num = random.randint(0, len(data)-1)
    msg_to_send = data[line_num]
    return msg_to_send

#print (pick_random_line(data))

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


futures = dict()

def get_callback(f, data):
    def callback(f):
        try:
            print(f.result())
            futures.pop(data)
        except:  # noqa
            print("Please handle {} for {}.".format(f.exception(), data))

    return callback

for i in range(num_msg_to_send):
    data = str(pick_random_line(f['data']))
    futures.update({data: None})
    # When you publish a message, the client returns a future.
    future = publisher.publish(
        topic_path, data=data.encode("utf-8")  # data must be a bytestring.
    )
    futures[data] = future
    # Publish failures shall be handled in the callback function.
    future.add_done_callback(get_callback(future, data))
    #sleep for a moment
    time.sleep(wait_time)

# Wait for all the publish futures to resolve before exiting.
while futures:
    time.sleep(5)

print("Published message with error handler.")