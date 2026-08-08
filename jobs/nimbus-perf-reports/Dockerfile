FROM continuumio/miniconda3
WORKDIR /app

# Install dependencies for gsutil
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-sdk \
    && apt-get clean

# Create the conda environment and install python dependencies
COPY requirements.txt .
RUN conda create --name nimbusperf python=3.10 && \
    conda run -n nimbusperf pip install --no-cache-dir -r requirements.txt

COPY . .
ENV BUCKET_URL="gs://moz-fx-data-prot-nonprod-c3a1-protodash/perf-reports"
CMD ["conda", "run", "--no-capture-output", "-n", "nimbusperf", "/app/entry.sh"]