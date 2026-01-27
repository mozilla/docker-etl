docker build -t ads-attribution-dap-collector .

docker run -it --rm \
  -v $HOME/.config/gcloud:/app/.config/gcloud \
  -e GOOGLE_CLOUD_PROJECT=<dev-project> \
  -e GOOGLE_APPLICATION_CREDENTIALS=<credentials>.json \
  ads-attribution-dap-collector python -m ads_attribution_dap_collector.main \
        --job_config_gcp_project <dev-project> \
        --bq_project <dev-project> \
        --job_config_bucket <attribution_config_bucket> \
        --bearer_token $DAP_BEARER_TOKEN \
        --hpke_private_key $DAP_PRIVATE_KEY \
        --process_date $PROCESS_DATE