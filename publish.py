
from log_generator import generate_log
import logging
from google.cloud import pubsub_v1
import random
import time


PROJECT_ID="info-i535"
TOPIC = "userlogs"


publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC)



def publish(publisher, topic, message):
    #UTF encoding the message and publishing it to Pub/Sub topic
    data = message.encode('utf-8')
    return publisher.publish(topic_path, data = data)



def callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic_name, message_future.exception()))
    else:
        print(message_future.result())


if __name__ == '__main__':

    while True:
        log = generate_log()
        print(log)
        message_future = publish(publisher, topic_path, log)
        message_future.add_done_callback(callback)
        sleep_time = random.choice(range(1, 3, 1))
        time.sleep(sleep_time)