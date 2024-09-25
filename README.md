# 아파트 실거래 데이터 수집 및 알림
## Setting
1. 환경변수로 아래 변수들을 생성해둘 것
- `PUBLIC_DATA_API_KEY` : 공공데이터 API KEY
- `TELEGRAM_BOT_TOKEN` : 텔레그램 봇 토큰
- `TELEGRAM_MONTHLY_CHAT_ID` : 월별 집계 데이터를 받을 텔레그램 `chat_id`
- `TELEGRAM_DETAIL_CHAT_ID` : 실거래 상세 데이터를 받을 텔레그램 `chat_id`
- `TELEGRAM_TEST_CHAT_ID` : 테스트 및 로깅용 `chat_id`

2. cron job을 실행할 cron script 생성
`.scheduler`를 파일이름으로 하여 아래처럼 crontab을 추가할 것
```plain
0 8-24/3 * * * /Users/user/.base/bin/python3.10 /Users/user/Desktop/apt_trade/src/apt_trade.py >> /Users/user/Desktop/apt_trade/cron.log 2>&1
1 8-24/3 * * * /Users/user/.base/bin/python3.10 /Users/user/Desktop/apt_trade/src/bunyang_trade.py >> /Users/user/Desktop/apt_trade/cron.log 2>&1
2 8-24/3 * * * /Users/user/.base/bin/python3.10 /Users/user/Desktop/apt_trade/src/sales.py >> /Users/user/Desktop/apt_trade/cron.log 2>&1
3 8-24/3 * * * /Users/user/.base/bin/python3.10 /Users/user/Desktop/apt_trade/src/notifier.py >> /Users/user/Desktop/apt_trade/cron.log 2>&1

```
```bash
crontab .scheduler
```

## Docker로 실행
```bash
docker-copmose up -d
```
docker에서는 `.env`, `.scheduler`를 사전에 작성해두고 컨테이너를 띄운다

또는 Dockerfile이나 compose yaml을 통해 환경변수 전달
