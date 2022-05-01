from google.cloud import pubsub_v1
from tqdm import tqdm
import json
import arrow
import random

PROJECT_ID = 'adroit-hall-301111'
TOPIC_ID = 'test-papermill'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

current_time = round(arrow.utcnow().timestamp())
data = [
        {'user': 'A', 'purchase_value': 213, 'timestamp': current_time + 0}, 
        {'user': 'A', 'purchase_value': 35, 'timestamp': current_time + 2}, 
        {'user': 'A', 'purchase_value': 53, 'timestamp': current_time + 4}, 
        {'user': 'A', 'purchase_value': 90, 'timestamp': current_time + 11},
        {'user': 'B', 'purchase_value': 2, 'timestamp': current_time + 0}, 
        {'user': 'B', 'purchase_value': 26, 'timestamp': current_time + 2}, 
        {'user': 'B', 'purchase_value': 3, 'timestamp': current_time + 4}, 
        {'user': 'B', 'purchase_value': 7, 'timestamp': current_time + 11},
]
        
for n in tqdm(data):

    # Add two attributes, origin and username, to the message
    future = publisher.publish(
        topic_path, json.dumps(n).encode("utf-8"), origin="python-sample", username="gcp"
    )
    
    future.result()