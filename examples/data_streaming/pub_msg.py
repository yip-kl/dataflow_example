from google.cloud import pubsub_v1
import arrow
import json
from tqdm import tqdm
import argparse

PROJECT_ID = 'adroit-hall-301111'
TOPIC_ID = 'test-papermill'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

parser = argparse.ArgumentParser()
parser.add_argument('count', type=int)

args = parser.parse_args()
for n in tqdm(range(args.count)):
    data = {
        'message_number': f'message={n}',
        'timestamp': round(arrow.utcnow().timestamp()),
        'products':[ 
            {'id': '1', 'name': 'abc'},
            {'id': '2', 'name': 'def'}
            ]
        }
    # Add two attributes, origin and username, to the message
    future = publisher.publish(
        topic_path, json.dumps(data).encode("utf-8"), origin="python-sample", username="gcp"
    )
    
    future.result()