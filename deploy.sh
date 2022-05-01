# See acceptable arguments here https://beam.apache.org/releases/pydoc/2.0.0/_modules/apache_beam/options/pipeline_options.html
# If the pipeline is to be updated, pass --update flag, see here https://cloud.google.com/dataflow/docs/guides/updating-a-pipeline
# Note: save_main_session is a very important flag so that the objects in the session e.g. imported packages, functions, etc. can be persisted across instances
python ./src/beam_job.py \
--runner DataflowRunner \
--project adroit-hall-301111 \
--region us-central1 \
--requirements_file ./src/requirements.txt \
--temp_location gs://adroit-hall-301111-dataflow/ \
--job_name dataflow-custom-pipeline-v1 \
--max_num_workers 2 \
--save_main_session \
--update
