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
COPY setup.py setup.py
COPY graphics_dashboard graphics_dashboard
RUN pip install --no-cache-dir .

RUN chown -R ${USER_ID}:${GROUP_ID} ${HOME}
USER ${USER_ID}

# The image runs two jobs; the caller selects the module:
#   python -m graphics_dashboard.dashboard   # the *-statistics / windows-features files
#   python -m graphics_dashboard.trends      # the weekly trend-*-v2 files
ENTRYPOINT ["python", "-m"]
CMD ["graphics_dashboard.dashboard"]
