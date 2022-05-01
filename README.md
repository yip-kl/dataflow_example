# Dataflow Examples
This is a demostration of the Dataflow, examples are under ```examples``` folder

## Examples
- Data streaming: Send data to Pub/Sub, then process them and import records to BigQuery
- Windowed aggregation: Aggregate purchase value by users from Pub/Sub event stream

## How to execute this
1. (Optional, for data_streaming only) Create a BQ table with definition stated in ```bq_table_def.json```
2. Spin up the Dataflow job
3. Publish message to the designated Pub/Sub topic
4. If all is good, output requirements.txt, then deploy to GCP with ```./deploy.sh```