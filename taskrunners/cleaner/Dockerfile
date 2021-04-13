FROM python:3.8.6
WORKDIR /usr
COPY requirements.txt requirements.txt
COPY setup_requirements.sh setup_requirements.sh
RUN ./setup_requirements.sh
WORKDIR /usr/src
COPY src .
CMD python3 main.py
