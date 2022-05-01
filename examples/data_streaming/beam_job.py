import argparse
import json
import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import arrow

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Service account key path
INPUT_SUBSCRIPTION = "projects/adroit-hall-301111/subscriptions/test-papermill-sub"
BIGQUERY_TABLE = "adroit-hall-301111:demo.pubsub_dataflow"
BIGQUERY_SCHEMA = "message_number:STRING, timestamp:TIMESTAMP, parsed_number:INT"

def parse_query_string(query_string, key):
    if match := re.search(f'{key}=([^&]*)', query_string, re.IGNORECASE):
        return match.group(1)

class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """  

    def process(self, element: bytes):
        """
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        data = json.loads(element.decode("utf-8"))
        data['parsed_number'] = parse_query_string(data['message_number'], 'message')
        data['parsed_timestamp'] = arrow.get(data['timestamp']).to('+08:00').format('YYYY-MM-DD HH:mm:ss')
        yield data

def run():
    # Parsing arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table", help="Output BigQuery Table", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="Output BigQuery Schema in text format",
        default=BIGQUERY_SCHEMA,
    )
    known_args, pipeline_args = parser.parse_known_args()

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        transformation = (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

if __name__ == "__main__":
    run()
