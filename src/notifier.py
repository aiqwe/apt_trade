import pandas as pd
from utils.utils import find_file, send, send_log, batch_manager, get_task_id, load_env
from utils.template import TelegramTemplate
from jinja2 import Template
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from utils.config import PathDictionary
import asyncio


def _prepare_dataframe(month: str, date_id: str):
    fpath = find_file(f"{month}.csv")
    df = pd.read_csv(fpath)

    df = df[df["date_id"] == date_id]
    if len(df) == 0:
        error_msg = f"No data found for {date_id}"
        asyncio.run(send_log(error_msg))
        raise ValueError(error_msg)
    return df


def daily_aggregation(month: str, date_id: str, sgg_contains: list = None):
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    df = _prepare_dataframe(month=month, date_id=date_id)
    last_df = _prepare_dataframe(month=month, date_id=prev_date_id)
    total = df["계약일"].count()
    last_total = last_df["계약일"].count()
    agg = (
        df[df["시군구코드"].isin(sgg_contains)]
        .groupby("시군구코드")[["계약일", "계약해지여부"]]
        .count()
        .reset_index()
    )
    change = total - last_total

    message = Template(TelegramTemplate.DAILY_STATUS).render(
        date_id=date_id,
        month=f"{str(month)[:4]}-{str(month)[4:]}",
        total_trade=total,
        change=change,
        sgg_list=agg["시군구코드"].to_list(),
        apt_trade=agg["계약일"].to_list(),
        apt_trade_cancels=agg["계약해지여부"].to_list(),
        zip=zip,
    )
    return message


def daily_specific_apt(month: str, date_id: str, apt_contains: list = None):
    df = _prepare_dataframe(month=month, date_id=date_id)
    apt_contains = [
        "헬리오시티",
        "마포래미안푸르지오",
        "마포프레스티지자이",
        "더클래시",
        "올림픽파크포레온",
        "잠실엘스",
        "리센츠",
        "파크리오",
        "고덕그라시움",
        "고덕아르테온",
        "옥수하이츠",
    ]
    df = pd.concat([df[df["아파트명"].str.contains(name)] for name in apt_contains])
    df["계약시점"] = (
        df["계약년도"].astype(str)
        + "-"
        + df["계약월"].astype(str).apply(lambda x: x.rjust(2, "0"))
        + "-"
        + df["계약일"].astype(str).apply(lambda x: x.rjust(2, "0"))
    )
    df["전용면적"] = df["전용면적"].apply(lambda x: f"{int(x)}({int((x / 3.3) + 7)}평)")
    df["거래금액"] = df["거래금액"].apply(
        lambda x: f"{round(int(x.replace(',', '')) / 1e4, 2)}억"
    )
    df = df[
        [
            "아파트명",
            "계약시점",
            "전용면적",
            "층",
            "거래금액",
            "시군구코드",
            "법정동",
            "거래유형",
            "계약해지사유발생일",
            "등기일자",
            "매수자",
            "매도자",
        ]
    ]
    df = df.sort_values(["아파트명", "전용면적", "계약시점", "층"])


if __name__ == "__main__":
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    date_id = datetime.now().strftime("%Y-%m-%d")
    sgg_contains = ["서초구", "강남구", "송파구", "마포구", "용산구", "성동구"]
    # chat_id = load_env("TELEGRAM_CHAT_ID", ".env", start_path=PathDictionary.root)
    chat_id = load_env("TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathDictionary.root)
    block = False

    batch_manager(
        task_id=get_task_id(__file__, last_month, "daily_status"),
        key=date_id,
        func=send,
        if_message=True,
        text=daily_aggregation(last_month, date_id=date_id, sgg_contains=sgg_contains),
        chat_id=chat_id,
        block=block,
    )
    batch_manager(
        task_id=get_task_id(__file__, this_month, "daily_status"),
        key=date_id,
        func=send,
        if_message=True,
        text=daily_aggregation(this_month, date_id=date_id, sgg_contains=sgg_contains),
        chat_id=chat_id,
        block=block,
    )
