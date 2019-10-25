FROM python:3.7-slim

COPY ./service /service
WORKDIR /service

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y -qq gcc
RUN pip install -r requirements.txt

EXPOSE 5000/tcp

ENTRYPOINT ["python3"]
CMD ["cim-ecp-service.py"]
