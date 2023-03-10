FROM python:3.9 AS builder
ADD ./requirements.txt .

RUN pip install --user -r requirements.txt

FROM python:3.9-slim
WORKDIR /root/bot
MAINTAINER limbend <limbeend@gmail.com>
LABEL version="1"
ENV TZ=Europe/Moscow

COPY --from=builder /root/.local /root/.local

ADD ./ /root/bot/

RUN apt-get update && apt-get install -y libpq5

CMD [ "python", "-u", "./bot.py"]