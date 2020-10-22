FROM python:3.8.6
WORKDIR /
ADD . /code
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
