FROM python:3

RUN mkdir /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install --upgrade pip setuptools wheel
RUN pip3 install -r requirements.txt

COPY . /app

EXPOSE 8500

ENTRYPOINT [ "python3" ]

CMD [ "producer.py" ]

