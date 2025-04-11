FROM python:3.11-slim
LABEL maintainer="Katelyn Windau <kwindau@mozilla.com>"

# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

RUN pip install --upgrade pip

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    unzip \
    curl \
    gnupg \
    ca-certificates \
    libglib2.0-0 \
    libgconf-2-4 \
    libfontconfig1 \
    libxss1 \
    libappindicator1 \
    libappindicator3-1 \
    libxrandr2 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    fonts-liberation \
    libdbus-glib-1-2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libu2f-udev \
    xdg-utils \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Install specific version of Chrome (v114.0.5735.90) via dpkg, then fix dependencies
RUN curl -sSL https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_114.0.5735.90-1_amd64.deb -o chrome.deb && \
    apt-get update && \
    dpkg -i chrome.deb || true && \
    apt-get -f install -y && \
    rm chrome.deb

# Install matching version of ChromeDriver (v114.0.5735.90)
RUN wget -O /tmp/chromedriver.zip https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/chromedriver && \
    rm /tmp/chromedriver.zip

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Copy app code
COPY . /app
WORKDIR /app

# Run your script
# CMD ["python", "extensions/main.py"] 
# Commented out since we will run the command from airflow with the passed in --date {{ ds }}
