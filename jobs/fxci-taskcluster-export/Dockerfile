FROM python:3.11
LABEL maintainer="Andrew Halberstadt <ahal@mozilla.com>"

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"
WORKDIR ${HOME}
ENV PATH="/app/.local/bin:$PATH"

RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

COPY . .

# Drop root and change ownership of the application folder to the user
RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

RUN pip install --no-cache-dir -r requirements/base.txt
RUN pip install --no-cache-dir --no-deps .
