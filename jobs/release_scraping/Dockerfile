FROM python:3.11-slim

LABEL maintainer="Katelyn Windau <kwindau@mozilla.com>"

# Set user and home
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"
ENV HOME=${HOME}

# Create non-root app user
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

RUN pip install --upgrade pip

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Drop root privileges
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Copy application code
COPY . /app
WORKDIR /app
