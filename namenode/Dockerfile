FROM python:3.7

#WORKDIR /app

COPY . /

RUN pip3 install -r requirements.txt

EXPOSE 2131
#WORKDIR /app/namenode
CMD ["python3", "master.py"]