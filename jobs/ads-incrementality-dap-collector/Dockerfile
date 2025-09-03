FROM python:3.12
LABEL maintainer="Glenda Leonard <gleonard@mozilla.com>"
ARG HOME="/janus_build"
WORKDIR ${HOME}

RUN apt update && apt --yes install curl

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH=$HOME/.cargo/bin:$PATH

# build the CLI tool
RUN git clone --depth 1 https://github.com/divviup/janus.git --branch '0.7.69'
RUN cd janus && cargo build -r -p janus_tools --bin collect

######### next stage

FROM python:3.12
LABEL maintainer="Glenda Leonard <gleonard@mozilla.com>"
# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"
WORKDIR ${HOME}

RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}
##################### from other Dockerfile
COPY --from=0 /janus_build/janus/target/release/collect ./
###################

COPY . .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir .
