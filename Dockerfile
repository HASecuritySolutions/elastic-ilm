FROM python:3.8.7-slim

LABEL description="H & A Security Solutions Elastic ILM"
LABEL maintainer="Justin Henderson -justin@hasecuritysolutions.com"

RUN apt update&& \
    apt install git pipenv -y && \
    apt clean

RUN cd /opt && \
    git clone https://github.com/HASecuritySolutions/elastic-ilm.git && \
    cd /opt/elastic-ilm

COPY ./settings.toml.example /opt/elastic-ilm/settings.toml
COPY ./client.json.example /opt/elastic-ilm/client.json

RUN cd /opt/elastic-ilm && \
    pip install -r requirements.txt && \
    useradd -ms /bin/bash elastic-ilm && \
    chown -R elastic-ilm:elastic-ilm /opt/elastic-ilm

WORKDIR /opt/elastic-ilm

USER elastic-ilm
STOPSIGNAL SIGTERM

CMD /usr/bin/python /opt/elastic-ilm/ilm.py --manual 1
