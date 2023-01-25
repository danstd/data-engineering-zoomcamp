FROM postgres:13

RUN apt-get update -q -y
RUN apt-get install libpq-dev gcc -q -y
RUN apt-get install python3 -q -y
RUN apt-get install pip -q -y
RUN pip install pandas sqlalchemy psycopg2 pyarrow

COPY .env .env