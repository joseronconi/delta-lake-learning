FROM deltaio/delta-docker
USER root
RUN apt-get update && apt-get install -y libpq-dev build-essential curl
RUN pip install --upgrade pip
RUN pip install delta-spark delta
