FROM python:3.8-alpine

RUN adduser -Dh /app app
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENTRYPOINT ["./barito_curator.py"]
