FROM python:3.11-slim

FROM astrocrpublic.azurecr.io/runtime:3.1-11

# USER root
# RUN apt install -y curl
# RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update

RUN pip install --no-cache-dir --upgrade -r requirements.txt