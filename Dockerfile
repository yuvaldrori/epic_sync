FROM python:2.7

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
    python-opencv \
    && apt-get autoremove \
    && apt-get clean

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY main.py .

CMD ["python", "main.py"]
