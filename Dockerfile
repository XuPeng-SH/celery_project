FROM python:3.6
RUN mkdir /source
WORKDIR /source
ADD ./requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
RUN python setup.py install
RUN rm -rf /source
