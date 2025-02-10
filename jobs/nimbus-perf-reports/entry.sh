#!/bin/bash
set -euxo pipefail
export PATH=/opt/conda/envs/nimbusperf/bin:$PATH

# Clear any pre-existing reports
rm -rf reports

echo -e "\n*************************************************"
echo -e "Copying index.html\n"
gsutil cp $BUCKET_URL/index.html index.html
cp index.html index-backup.html

echo -e "\n*************************************************"
echo -e "Generating reports...\n"
python find-latest-experiment index.html

if [ -d "reports" ] && [ "$(ls -A reports)" ]; then
  for file in reports/* ; do
    echo -e "\n*************************************************"
    echo -e "Updating index.html with $file"
    python update-index --index index.html --append $file

    echo -e "Transferring $file to protosaur\n"
    gsutil cp $file $BUCKET_URL/$(basename $file)
  done

  echo -e "\n*************************************************"
  echo -e "Uploading new index.html to protosaur\n"
  gsutil cp index-backup.html $BUCKET_URL/index-backup.html
  gsutil cp index.html $BUCKET_URL/index.html
fi
