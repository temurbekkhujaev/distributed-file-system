FROM python:3.7

#WORKDIR /app

#COPY . /app

RUN pip3 install -r requirements.txt

EXPOSE 2131
#WORKDIR /app/namenode

CMD ["python", "storage.py", "http://namenode:2131", /"]