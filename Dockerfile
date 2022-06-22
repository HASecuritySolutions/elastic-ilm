FROM python:3.11.0a7-slim

LABEL description="H & A Security Solutions Elastic ILM"
LABEL maintainer="Justin Henderson -justin@hasecuritysolutions.com"

RUN apt update && \
    apt install git pipenv build-essential libssl-dev libffi-dev python3-dev -y && \
    apt clean && \
    cd /opt && \
    git clone https://github.com/HASecuritySolutions/elastic-ilm.git && \
    cd /opt/elastic-ilm && \
    pip install --no-cache-dir -r requirements.txt && \
    useradd -ms /bin/bash elastic-ilm && \
    chown -R elastic-ilm:elastic-ilm /opt/elastic-ilm

COPY ./settings.toml.example /opt/elastic-ilm/settings.toml
COPY ./client.json.example /opt/elastic-ilm/client.json

WORKDIR /opt/elastic-ilm

USER elastic-ilm
STOPSIGNAL SIGTERM

CMD /usr/local/bin/python -u /opt/elastic-ilm/ilm.py --manual 1
