FROM python:3.14-slim-trixie
LABEL maintainer="bewu@mozilla.com"

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

# fetch_generated_sql.sh and fetch_metric_hub.sh clone with git
RUN apt-get -y update && apt-get -y install \
    git \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip --no-cache-dir

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

COPY . .

# script/run_all.sh defaults to a local .venv
ENV PY=python
# A container runs once, so the cache stays in memory rather than on disk
ENV METRIC_USAGE_CACHE=0

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

CMD ["script/run_all.sh"]
