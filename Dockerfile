FROM python:alpine

RUN pip3 install googlemaps PyMySQL geopy
COPY main.py /
CMD ["/usr/local/bin/python", "main.py"]
