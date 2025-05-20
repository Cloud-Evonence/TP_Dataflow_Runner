**This Apache Beam pipeline runs on Google Cloud Dataflow and performs the following tasks:**
    Reads raw weather data from BigQuery (Current_weather) Source Table.
    Groups data by city and date
    Calculates average temperature and windspeed
    Writes the aggregated results to a BigQuery (Current_weather_summary) Target Table.

**File Structure:**
    weather_daily_summary.py     # Main Beam pipeline
    README.md                    # Current File
    requirements.txt             # Beam dependency file

**GCS bucket:**
    Buckets: temp_area/ and staging_area/ folders
    Python file : gs://tp_datflow_job_script/scripts/weather_daily_summary.py

**Source Table Schema:**
    City STRING,
    Country STRING,
    latitude FLOAT64,
    longitude FLOAT64,
    Fetched_at Timestamp,
    Temperature FLOAT64,
    Windspeed FLOAT64,
    Observation_time Timestamp

**Target Table Schema:**
    city STRING,
    date DATE,
    avg_temperature FLOAT64,
    avg_windspeed FLOAT64

**How to RUN:**
    **From CloudShell or Local Terminal(After Uploading .py to GCS) Run:**
      python weather_daily_summary.py \
      --runner=DataflowRunner \
      --project=project-4-workndemos \
      --region=us-central1 \
      --temp_location=gs://tp_datflow_job_script/temp_area \
      --staging_location=gs://tp_datflow_job_script/staging_area
