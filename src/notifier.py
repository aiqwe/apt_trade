import asyncio
import telegram
import pandas as pd
from utils.utils import load_env, find_file
from utils.template import TelegramTemplate
from jinja2 import Template
from datetime import datetime
from dateutil.relativedelta import relativedelta

def generate_message(month):
    fpath = find_file(f"{month}.csv")
    df = pd.read_csv(fpath)

    date_id = df['date_id'].max()
    df = df[df['date_id'] == date_id]
    agg = df.groupby('시군구코드')[['계약일', '계약해지여부']].count().reset_index()
    total = df['계약일'].count()

    message = Template(TelegramTemplate.DAILY_TRADE).render(
        date_id=date_id,
        month=f"{str(month)[:4]}-{str(month)[4:]}",
        total_trade=total,
        sgg_list=agg['시군구코드'].to_list(),
        apt_trade=agg['계약일'].to_list(),
        apt_trade_cancels=agg['계약해지여부'].to_list(),
        zip=zip)
    return message


async def send(text: str, chat_id: str, token: str):
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=text)


if __name__ == '__main__':
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    token = load_env("TELEGRAM_BOT_TOKEN", ".env")
    chat_id = "-1002254050157"

    asyncio.run(send(text=generate_message(this_month), chat_id=chat_id, token=token))
    asyncio.run(send(text=generate_message(last_month), chat_id=chat_id, token=token))