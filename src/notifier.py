from utils.slack import send_message, BlockTemplate
from jinja2 import Template
from utils.utils import find_file
import pandas as pd
import json
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta

# daily 거래량
def daily(month):
    fpath = find_file(f"{month}.csv")
    df = pd.read_csv(fpath)

    date_id = df['date_id'].max()
    df = df[df['date_id'] == date_id]
    agg = df.groupby('시군구코드')[['계약일', '계약해지여부']].count().reset_index()
    total = df['계약일'].count()

    message = Template(BlockTemplate.DAILY_MESSAGE).render(
        date_id=date_id,
        month=f"{str(month)[:4]}-{str(month)[4:]}",
        total_trade=total,
        sgg_list=agg['시군구코드'].to_list(),
        apt_trade=agg['계약일'].to_list(),
        apt_trade_cancels=agg['계약해지여부'].to_list(),
        zip=zip)
    blocks = json.loads(message)['blocks']
    send_message(blocks=blocks)
    logger.info("send message")

if __name__ == '__main__':
    # Daily 거래량 알림
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))

    daily(last_month)
    daily(this_month)
