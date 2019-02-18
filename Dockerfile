FROM python:3.6.7
WORKDIR /usr/src/app
COPY . .
RUN pip install -r ./requirements.txt