FROM python:3.11-slim-bookworm

LABEL maintainer="Katelyn Windau <kwindau@mozilla.com>"

ENV DEBIAN_FRONTEND=noninteractive

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

# Install system dependencies + Chromium
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    gnupg \
    ca-certificates \
    chromium \
    chromium-driver \
    fonts-liberation \
    libglib2.0-0 \
    libnss3 \
    libxss1 \
    libayatana-appindicator3-1 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libasound2 \
    libgbm1 \
    libu2f-udev \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxtst6 \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*


# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Drop root privileges
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Copy application code
COPY . /app
WORKDIR /app
