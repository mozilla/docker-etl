# The statistics engine, taken from GrowthBook's published image at a frozen digest. The PyPI
# package of the same version pins a pandas that conflicts with the one this job runs on, so the
# image is the source. Only the Python package is copied; scipy, the one dependency it lacks, is
# declared in requirements.txt.
FROM growthbook/growthbook@sha256:48c506939021bb04cf16457f1814b1699c1c2747a0618c73e7a2ffcff31c4ee1 AS gbstats

# 3.11, because gbstats is copied in below as a frozen Python 3.11 site-packages tree.
FROM python:3.11-slim

ARG USER_ID="10001"
ARG GROUP_ID="app"
ARG HOME="/app"

ENV HOME=${HOME}
RUN groupadd --gid ${USER_ID} ${GROUP_ID} && \
    useradd --create-home --uid ${USER_ID} --gid ${GROUP_ID} --home-dir ${HOME} ${GROUP_ID}

WORKDIR ${HOME}

# The literal python3.11 rather than an interpolated version: the base image sets its own
# PYTHON_VERSION to a full 3.11.x, which shadows a build ARG of the same name and leaves the COPY
# pointing at a path that does not exist.
COPY --from=gbstats /opt/venv/lib/python3.11/site-packages/gbstats /opt/gbstats/gbstats
COPY --from=gbstats /opt/venv/lib/python3.11/site-packages/gbstats-0.8.0.dist-info \
  /opt/gbstats/gbstats-0.8.0.dist-info
ENV PYTHONPATH=/opt/gbstats

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY .flake8 .flake8
COPY pytest.ini pytest.ini
COPY setup.py setup.py
COPY highwind highwind
COPY tests tests
RUN pip install --no-cache-dir .

RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# Entrypoint is bare `python` so CI can run `-m pytest` / `-m flake8` against the
# same image. Normal runs take the CMD below.
ENTRYPOINT ["python"]
CMD ["-m", "highwind.main"]
