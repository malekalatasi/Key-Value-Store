FROM python:alpine3.7

COPY . /REST

WORKDIR /REST

RUN pip install -r requirements.txt

EXPOSE 8085

ENTRYPOINT [ "python3" ]

CMD [ "main.py" ]


