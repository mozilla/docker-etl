FROM python:3.10
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

FROM python:3.10
LABEL maintainer="Glenda Leonard <gleonard@mozilla.com>"
# https://github.com/mozilla-services/Dockerflow/blob/master/docs/building-container.md
ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"
ENV HOME=${HOME}

RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}
COPY --from=0 /janus_build/janus/target/release/collect ./

RUN pip install --upgrade pip

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

RUN pip install .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}
