# base image
FROM python:3.8-slim-buster

# # install dependencies
# RUN apk update && \
#     apk add --update alpine-sdk && \
#     apk add gcc && \
#     apk add python3-dev && \
#     apk add --virtual build-deps gcc python3-dev musl-dev && \
#     apk add postgresql-dev && \
#     apk add mariadb-dev && \
#     apk add freetype-dev && \
#     apk add libpng && \
#     apk add netcat-openbsd && \
#     apk add libffi-dev && \
#     apk add libxml2-dev && \
#     apk add libxslt-dev

# set working directory
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# DBGEN
COPY ./dbgen.tar.gz /tmp/dbgen.tar.gz
RUN pip install  /tmp/dbgen.tar.gz
COPY config/dbgen_files /usr/src/app/dbgen_files

ARG DEFAULT_ENV
ENV DEFAULT_ENV $DEFAULT_ENV


# add entrypoint.sh
COPY ./script/entrypoint.sh /usr/src/app/entrypoint.sh
RUN chmod +x /usr/src/app/entrypoint.sh
RUN mkdir -p /usr/src/app/dbgen_temp


# Set entrypoint
WORKDIR /usr/src/app
ENTRYPOINT ["./entrypoint.sh"]
CMD ["sh"] # set default arg for entrypoint
