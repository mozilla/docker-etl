FROM python:3.14-slim

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY .flake8 .flake8
COPY pytest.ini pytest.ini
COPY setup.py setup.py
COPY crash_missing_symbols crash_missing_symbols
COPY tests tests
RUN pip install --no-cache-dir .

RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Entrypoint is bare `python` so CI can run `-m pytest` / `-m flake8` against the
# same image. Normal runs take the CMD below.
ENTRYPOINT ["python"]
CMD ["-m", "crash_missing_symbols.main"]
