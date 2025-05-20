import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_keyed(record):
    try:
        city = record['city']
        date = record['fetched_at'].date().isoformat()
        return ((city, date), (record['temperature'], record['windspeed'], 1))
    except Exception as e:
        logger.error(f"Error in extract_keyed: {e} | record: {record}")
        raise


def compute_avg(values):
    try:
        sum_temp, sum_wind, count = 0.0, 0.0, 0
        for t, w, c in values:
            sum_temp += t
            sum_wind += w
            count += c
        if count == 0:
            return {'avg_temperature': None, 'avg_windspeed': None}
        return {
            'avg_temperature': sum_temp / count,
            'avg_windspeed': sum_wind / count
        }
    except Exception as e:
        logger.error(f"Error in compute_avg: {e}")
        raise


def format_row(kv):
    try:
        (city, date), agg = kv
        return {
            'city': city,
            'date': date,
            'avg_temperature': agg['avg_temperature'],
            'avg_windspeed': agg['avg_windspeed']
        }
    except Exception as e:
        logger.error(f"Error in format_row: {e} | input: {kv}")
        raise

def run():
    logger.info("Starting Dataflow pipeline...")

    options = PipelineOptions(
        runner='DataflowRunner',
        project='project-4-workndemos',
        region='us-central1',
        temp_location='gs://tp_datflow_job_script/temp_area',
        staging_location='gs://tp_datflow_job_script/staging_area',
        job_name='dataflow-runner',
        save_main_session=True
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(
                query='''
                    SELECT city, temperature, windspeed, fetched_at
                    FROM `project-4-workndemos.TP_api_integration.current_weather`
                    WHERE DATE(fetched_at) < CURRENT_DATE()
                ''',
                use_standard_sql=True
            )
            | 'Extract Key' >> beam.Map(extract_keyed)
            | 'Group by city+date' >> beam.GroupByKey()
            | 'Compute Averages' >> beam.Map(lambda kv: (kv[0], compute_avg(kv[1])))
            | 'Format Output' >> beam.Map(format_row)
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'project-4-workndemos.TP_api_integration.current_weather_summary',
                schema='city:STRING, date:DATE, avg_temperature:FLOAT64, avg_windspeed:FLOAT64',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

    logger.info("Pipeline completed successfully.")

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise