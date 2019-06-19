FROM python:3.6
RUN mkdir /source
WORKDIR /source
COPY . .
RUN python setup.py install
RUN rm -rf /source
