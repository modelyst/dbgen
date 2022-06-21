ARG PYTHON_VERSION="3.9"

FROM python:${PYTHON_VERSION}-slim as builder

ENV PYTHONFAULTHANDLER=1 \
    PYTHONHASHSEED=random \
    PYTHONUNBUFFERED=1

ARG RUNTIME_APT_DEPS="\
    postgresql \
    libpq-dev"

ENV RUNTIME_APT_DEPS=${RUNTIME_APT_DEPS} \
    POETRY_VERSION==1.2.0b2

ARG ADDITIONAL_RUNTIME_APT_DEPS=""
ENV ADDITIONAL_RUNTIME_APT_DEPS=${ADDITIONAL_RUNTIME_APT_DEPS}

ARG RUNTIME_APT_COMMAND="echo"
ENV RUNTIME_APT_COMMAND=${RUNTIME_APT_COMMAND}

ARG ADDITIONAL_RUNTIME_APT_COMMAND=""
ENV ADDITIONAL_RUNTIME_APT_COMMAND=${ADDITIONAL_RUNTIME_APT_COMMAND}

ARG ADDITIONAL_RUNTIME_APT_ENV=""

RUN mkdir -pv /usr/share/man/man1 \
    && mkdir -pv /usr/share/man/man7 \
    && export ${ADDITIONAL_RUNTIME_APT_ENV?} \
    && bash -o pipefail -e -u -x -c "${RUNTIME_APT_COMMAND}" \
    && bash -o pipefail -e -u -x -c "${ADDITIONAL_RUNTIME_APT_COMMAND}" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    ${RUNTIME_APT_DEPS} \
    ${ADDITIONAL_RUNTIME_APT_DEPS} \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ARG DBGEN_HOME=/home/dbgen-user
ENV DBGEN_HOME=${DBGEN_HOME}
WORKDIR ${DBGEN_HOME}

# Install poetry
RUN pip install "poetry==$POETRY_VERSION"
# Add the lock file
COPY poetry.lock pyproject.toml  ${DBGEN_HOME}/


# Set environment
ARG YOUR_ENV="production"
ENV YOUR_ENV=${YOUR_ENV}
ARG EXTRAS
ENV EXTRAS ${EXTRAS}
# Project initialization:
# Need to touch these files or poetry complains!
RUN mkdir -p ${DBGEN_HOME}/src/dbgen && touch ${DBGEN_HOME}/src/dbgen/__init__.py && touch ${DBGEN_HOME}/README.md
RUN poetry config virtualenvs.in-project true \
    && poetry install --without dev -E boto3
ENV PYTHONPATH=${DBGEN_HOME}:${PYTHONPATH}

FROM python:${PYTHON_VERSION}-slim as final
ARG DBGEN_HOME=/home/dbgen-user
WORKDIR ${DBGEN_HOME}
COPY --from=builder ${DBGEN_HOME}/.venv ${DBGEN_HOME}/.venv
ENV PATH="${DBGEN_HOME}/.venv/bin:$PATH"
ARG RUNTIME_APT_DEPS="\
    dumb-init \
    postgresql-client \
    libpq-dev \
    sudo \
    git"
RUN apt-get update -y --no-install-recommends \
    && apt-get install -y --no-install-recommends $RUNTIME_APT_DEPS \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY ./src ./src
COPY scripts/in_container/entrypoint_prod.sh /entrypoint
# DBGEN
# Creating folders, and files for a project:
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
