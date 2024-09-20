FROM python:3.11-slim
ARG app=/root/apt_trade

RUN apt-get update && apt-get install cron cmake build-essential -y

COPY . $app

WORKDIR $app
RUN pip install -r requirements.txt

RUN crontab ./scheduler

EXPOSE 80 403
CMD cron && tail -f /dev/null