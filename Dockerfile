FROM python:3.13-alpine

ENV LANG=en_US.UTF-8
ENV PYTHONUNBUFFERED=1
ENV HOME=/cct

# Create a non-root user
ARG USERNAME=python
RUN adduser -g ${USERNAME} --disabled-password ${USERNAME}

WORKDIR ${HOME}

RUN apk update --no-cache && \
    apk upgrade --no-cache && \
    apk add --no-cache git

COPY process_globals.py .
COPY process_utils.py .
COPY process_incoming_data.py .

RUN chown -R ${USERNAME}:${USERNAME} ${HOME}
USER ${USERNAME}
