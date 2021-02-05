FROM python:3.9.0-slim

ENV http_proxy=http://proxy.klima.ads.local:2080/ \
    https_proxy=http://proxy.klima.ads.local:2080/ \
    no_proxy=10.3.0.0/16 \
    PYTHONPATH="/opt/dm_lab2fhir_inc"

WORKDIR /opt/dm_lab2fhir_inc
COPY dm_lab2fhir_inc/ /opt/dm_lab2fhir_inc/
RUN pwd && ls
RUN apt update && apt install -y git && pip install -U pytest && pip install pylint && pip3 install -r requirements

EXPOSE 5432 80

ENTRYPOINT ["pytest"]

LABEL org.opencontainers.image.authors="DBMI" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="DBMI" \
      org.opencontainers.image.title="dm_lab2fhir_inc"
