docker build -t ads_incrementality_dap_collector .

docker run -it --rm \
  -v $HOME/.config/gcloud:/app/.config/gcloud \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/.config/gcloud/application_default_credentials.json \
  ads_incrementality_dap_collector python ./ads_incrementality_dap_collector/main.py \
        --gcp_project moz-fx-dev-mlifshin-sandbox \
        --job_config_bucket ads-nonprod-stage-incrementality-dap-collector-config \
        --hpke_token $DAP_HPKE_TOKEN \
        --hpke_private_key  $DAP_PRIVATE_KEY \
        --batch_start $BATCH_START \
