FROM python:3-alpine

RUN pip3 install googlemaps PyMySQL geopy
WORKDIR /
COPY main.py /
CMD ["/usr/local/bin/python", "main.py"]
