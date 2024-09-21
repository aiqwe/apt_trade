import pandas as pd
from utils.utils import find_file, send, batch_manager, get_task_id
from utils.template import TelegramTemplate
from jinja2 import Template
from datetime import datetime
from dateutil.relativedelta import relativedelta


def daily_aggregation(month: str, date_id: str):
    fpath = find_file(f"{month}.csv")
    df = pd.read_csv(fpath)

    df = df[df["date_id"] == date_id]
    if len(df) == 0:
        error_msg = f"No data found for {date_id}"
        send(text=error_msg)
        raise ValueError(error_msg)
    agg = df.groupby("시군구코드")[["계약일", "계약해지여부"]].count().reset_index()
    total = df["계약일"].count()

    message = Template(TelegramTemplate.DAILY_TRADE).render(
        date_id=date_id,
        month=f"{str(month)[:4]}-{str(month)[4:]}",
        total_trade=total,
        sgg_list=agg["시군구코드"].to_list(),
        apt_trade=agg["계약일"].to_list(),
        apt_trade_cancels=agg["계약해지여부"].to_list(),
        zip=zip,
    )
    return message


if __name__ == "__main__":
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    date_id = datetime.now().strftime("%Y-%m-%d")

    batch_manager(
        task_id=get_task_id(__file__, last_month, "daily_status"),
        key=date_id,
        func=send,
        if_message=True,
        text=daily_aggregation(last_month, date_id=date_id),
    )
    batch_manager(
        task_id=get_task_id(__file__, this_month, "daily_status"),
        key=date_id,
        func=send,
        if_message=True,
        text=daily_aggregation(this_month, date_id=date_id),
    )
