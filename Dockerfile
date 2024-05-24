# docker build -t brian/sz_search_flask .
# docker run --user $UID -it -v $PWD:/data -e SENZING_ENGINE_CONFIGURATION_JSON brian/sz_search_flask /dev/null

ARG BASE_IMAGE=senzing/senzingapi-runtime:staging
FROM ${BASE_IMAGE}

LABEL Name="brain/sz_search_flask" \
      Maintainer="brianmacy@gmail.com" \
      Version="DEV"

RUN apt-get update \
 && apt-get -y install python3 python3-pip \
 && python3 -mpip install orjson \
 && apt-get -y remove build-essential python3-pip \
 && apt-get -y autoremove \
 && apt-get -y clean

COPY sz_search_flask.py /app/

ENV PYTHONPATH=/opt/senzing/g2/sdk/python
ENV LANGUAGE=C
ENV LC_ALL=C.UTF-8

USER 1001

WORKDIR /app
ENTRYPOINT ["/app/sz_search_flask.py"]

