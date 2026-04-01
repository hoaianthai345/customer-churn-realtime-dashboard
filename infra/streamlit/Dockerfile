FROM python:3.10-slim

WORKDIR /opt/project

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8501
