docker build -t ads_incrementality_dap_collector .

docker run -it --rm \
  -v $HOME/.config/gcloud:/app/.config/gcloud \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/.config/gcloud/application_default_credentials.json \
  ads_incrementality_dap_collector python ./ads_incrementality_dap_collector/main.py \
      --project_id moz-fx-dev-mlifshin-sandbox \
      --dataset_id ads_dap_aggregations \
      --table_id test \
      --experiment_slug $EXPERIMENT_SLUG \
      --hpke_token $DIVVIUP_HPKE_TOKEN \
      --hpke_private_key  $DIVVIUP_PRIVATE_KEY \
      --hpke_config $DIVVIUP_HPKE_CONFIG \
      --batch_start $BATCH_START \
      --batch_duration 3600
