FROM python:3.12.1-alpine3.19

RUN apk add --no-cache zfs

COPY . /opt/app
WORKDIR /opt/app
RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app/zpool-list.py"]