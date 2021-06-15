# base image
FROM python:3.8-slim-buster


ARG RUNTIME_APT_DEPS="\
    dumb-init \
    postgresql \
    postgresql-client \
    sudo "

ENV RUNTIME_APT_DEPS=${RUNTIME_APT_DEPS} \
    POETRY_VERSION==1.1.5

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

RUN apt-get install -y --no-install-recommends dumb-init

# set working directory
RUN mkdir -p /home/dbgen
WORKDIR /home/dbgen
# Install poetry
RUN pip install "poetry==$POETRY_VERSION"
# Add the lock file
COPY poetry.lock pyproject.toml /home/dbgen/

# Set environment
ARG YOUR_ENV="development"
ENV YOUR_ENV=${YOUR_ENV}

# Project initialization:
RUN poetry config virtualenvs.create false \
    && poetry install $(test $YOUR_ENV = "production" && echo "--no-dev")  --no-interaction --no-ansi

# add entrypoint.sh
COPY docker/config/dbgen_files /home/dbgen/dbgen_files
COPY scripts/in_container/entrypoint_prod.sh /entrypoint
RUN chmod a+x /entrypoint

# Set DBGEN EnvVars
ARG DBGEN_VERSION
ENV DBGEN_VERSION=${DBGEN_VERSION}
ARG DBGEN_HOME=/home/dbgen
ENV DBGEN_HOME=${DBGEN_HOME}
# DBGEN
COPY ./dist/dbgen-${DBGEN_VERSION}.tar.gz /tmp/dbgen.tar.gz
RUN pip install  /tmp/dbgen.tar.gz


# Make DBgen files belong to the root group and are accessible. This is to accommodate the guidelines from
# OpenShift https://docs.openshift.com/enterprise/3.0/creating_images/guidelines.html
RUN mkdir -pv "${DBGEN_HOME}"; \
    mkdir -pv "${DBGEN_HOME}/dags"; \
    mkdir -pv "${DBGEN_HOME}/logs"; \
    mkdir -pv "${DBGEN_HOME}/dbgen_src";

# Set entrypoint
WORKDIR /home/dbgen/dbgen_src/
# DBGEN
# Creating folders, and files for a project:
WORKDIR /home/dbgen/
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
