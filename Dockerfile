FROM python:3.9-slim

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . /src
WORKDIR /src

ENTRYPOINT ["bin/run_chord.sh"]
