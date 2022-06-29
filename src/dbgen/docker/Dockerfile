
ARG DBGEN_VERSION="latest"

FROM modelyst/dbgen:${DBGEN_VERSION}

ARG MODEL_FOLDER=""
ARG MODEL_NAME=""
ARG DATA_FOLDER=""
ARG ENV_FILE=""

COPY requirements.txt /home/dbgen-user/requirements.txt
RUN pip install -r requirements.txt
COPY ${MODEL_FOLDER} /home/dbgen-user/src/${MODEL_NAME}
COPY ${ENV_FILE} /home/dbgen-user/.env

CMD [ "run" ]
