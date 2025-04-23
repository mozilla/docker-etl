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
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libappindicator3-1 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libxtst6 \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Install Google Chrome 125.0.6422.60 manually 
RUN apt-get update && apt-get install -y wget curl unzip gnupg ca-certificates --no-install-recommends && \
    wget -O /tmp/google-chrome-stable.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_125.0.6422.60-1_amd64.deb && \
    apt-get install -y /tmp/google-chrome-stable.deb && \
    rm /tmp/google-chrome-stable.deb

# Install matching ChromeDriver 125.0.6422.60 from Chrome for Testing
RUN wget -O /tmp/chromedriver.zip https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/125.0.6422.60/linux64/chromedriver-linux64.zip && \
    unzip /tmp/chromedriver.zip -d /tmp && \
    mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /tmp/chromedriver-linux64

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Drop root privileges
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Copy application code
COPY . /app
WORKDIR /app

# CMD ["python", "extensions/main.py"]
