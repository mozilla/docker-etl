# Play Store Export

This Play Store export is a job to schedule backfills of Play Store data to BigQuery via the BigQuery Data Transfer service.

The purpose of this job is to continuously backfill past days over time.  
Past Play Store data has been found to still update over time 
(e.g. data from a day two weeks ago can still be updated)
so regular backfills of at least 30 days are required.  
The BigQuery Play Store transfer job has a non-configurable refresh 
window size of 7 days which is insufficient. 

See [Google Play transfers documentation](https://cloud.google.com/bigquery-transfer/docs/play-transfer) for more details.

https://googleapis.dev/python/bigquerydatatransfer/latest/index.html
