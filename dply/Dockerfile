FROM python:3.9.0-slim

ENV http_proxy=http://proxy.klima.ads.local:2080/ \
    https_proxy=http://proxy.klima.ads.local:2080/ \
    no_proxy=10.3.0.0/16

RUN echo "Acquire::http::Proxy \"http://193.197.95.183:2080/\";" >\
    /etc/apt/apt.conf.d/30proxy

WORKDIR /opt/dm_lab2fhir_inc
COPY dm_lab2fhir_inc/requirements .
COPY dm_lab2fhir_inc/lib lib
RUN apt-get update && apt-get install --no-install-recommends -y git=1:2.20.1-2+deb10u3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && pip3 install -r requirements

EXPOSE 5432

COPY dm_lab2fhir_inc/app.py .

CMD ["python", "./app.py", "", "", "", "", "", "", ""]

LABEL org.opencontainers.image.authors="DBMI" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="DBMI" \
      org.opencontainers.image.title="dm_lab2fhir_inc"