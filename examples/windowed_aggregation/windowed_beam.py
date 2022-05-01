import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger
import json

parser = argparse.ArgumentParser()
known_args, pipeline_args = parser.parse_known_args()

# Creating pipeline options
pipeline_options = PipelineOptions(pipeline_args, streaming = True)

class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """  

    def process(self, element: bytes):
        """
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        data = json.loads(element.decode("utf-8"))
        yield data

def run():
    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        events = (
            p | 'ReadFromPubSub' >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription='projects/adroit-hall-301111/subscriptions/test-papermill-sub', timestamp_attribute=None
            )
            
            | "Parse json" >> beam.ParDo(CustomParsing())
            
            # Set timestamp https://beam.apache.org/documentation/transforms/python/elementwise/withtimestamps/ 
            | 'Add Timestamps' >> beam.Map(lambda x: beam.window.TimestampedValue(x, x['timestamp'])) 
            
            # Define window https://beam.apache.org/documentation/programming-guide/#windowing
            | 'Add Window info' >>  beam.WindowInto(beam.window.FixedWindows(5)) 
            
            # Transform the data so that Key-related functions can be used
            | 'Turn into key-value pair' >> beam.Map(lambda x: (x['user'], x['purchase_value']))
            
            # Self-explanatory
            | 'Sum values per key' >> beam.CombinePerKey(sum)
            
            # Print the output
            | 'write session elements' >> beam.ParDo(print)
        )
    
if __name__ == "__main__":
    run()