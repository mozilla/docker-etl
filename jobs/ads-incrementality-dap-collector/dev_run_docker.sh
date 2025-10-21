docker build -t ads_incrementality_dap_collector .

docker run -it --rm \
  -v $HOME/.config/gcloud:/app/.config/gcloud \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/.config/gcloud/application_default_credentials.json \
  ads_incrementality_dap_collector python ./ads_incrementality_dap_collector/main.py \
        --job_config_gcp_project moz-fx-dev-username-sandbox \
        --job_config_bucket moz-fx-dev-username-sandbox-incrementality-dap-collector-config \
        --bearer_token $DAP_BEARER_TOKEN \
        --hpke_private_key  $DAP_PRIVATE_KEY \
        --process_date $PROCESS_DATE \
