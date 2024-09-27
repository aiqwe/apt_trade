import pandas as pd
from copy import deepcopy
from utils.utils import (
    find_file,
    send_message,
    send_log,
    BatchManager,
    get_task_id,
    load_env,
)
from utils.template import TelegramTemplate
from utils.processing import process_sales_column
from jinja2 import Template
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from utils.config import PathDictionary, FilterDictionary
import asyncio
from loguru import logger


def _prepare_dataframe(fname: str, date_id: str):
    fpath = find_file(f"{fname}")
    df = pd.read_csv(fpath)

    df = df[df["date_id"] == date_id]
    if len(df) == 0:
        error_msg = f"No data found for {date_id}"
        logger.error(error_msg)
        asyncio.run(send_log(error_msg))
        raise ValueError(error_msg)
    return df


def daily_aggregation(month: str, date_id: str, sgg_contains: list = None):
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    df = _prepare_dataframe(fname=f"trade_{month}.csv", date_id=date_id)
    last_df = _prepare_dataframe(fname=f"trade_{month}.csv", date_id=prev_date_id)
    total = df["계약일"].count()
    last_total = last_df["계약일"].count()
    agg = (
        df[df["시군구코드"].isin(sgg_contains)]
        .groupby("시군구코드")[["계약일", "계약해지여부", "신규거래"]]
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
        apt_trades=agg["계약일"].to_list(),
        new_trades=agg["신규거래"].to_list(),
        apt_trade_cancels=agg["계약해지여부"].to_list(),
        zip=zip,
    )
    return message


def daily_specific_apt(
    month: str, date_id: str, apt_contains: list = None, filter_new=True
):
    df = _prepare_dataframe(fname=f"trade_{month}.csv", date_id=date_id)
    if filter_new:
        df = df[df["신규거래"] == "신규"]
    if apt_contains:
        df = pd.concat([df[df["아파트명"].str.contains(name)] for name in apt_contains])
    df["전용면적"] = df["전용면적"].apply(lambda x: f"{int(x)}({int((x / 3.3) + 7)}평)")
    df["거래금액"] = df["거래금액"].apply(
        lambda x: f"{round(int(x.replace(',', '')) / 1e4, 2)}억"
    )
    cols = [
        "아파트명",
        "시군구코드",
        "법정동",
        "계약일",
        "전용면적",
        "층",
        "거래금액",
        "거래유형",
    ]
    df = df[cols]
    df = df.sort_values(["아파트명", "전용면적", "계약일", "층"])
    data = df[cols].to_dict(orient="records")

    message = Template(TelegramTemplate.DAILY_DIFFERENCE).render(
        month=f"{str(month)[:4]}-{str(month)[4:]}", date_id=date_id, data=data, len=len
    )
    return message


def sales_aggregation(month, date_id):
    df = _prepare_dataframe(fname=f"sales_{month}.csv", date_id=date_id)
    df = process_sales_column(df)
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # 현재로부터 7일 전까지 확인된 것
    def _agg(df, date_id):
        data = deepcopy(df)
        day_filter = (
            datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=7)
        ).strftime("%Y-%m-%d")
        data = data[data["확인날짜"] >= day_filter]

        grouped = data.groupby("단지명").agg(
            {"price": ["mean", "median", "max", "min", "count"]}
        )
        grouped.columns = ["평균", "중앙", "최대", "최저", "매물수"]
        grouped = grouped.reset_index()
        grouped = grouped.sort_values("평균")
        return grouped

    def _process_columns(df):
        res = deepcopy(df)
        res["매물수"] = res["매물수"].apply(lambda x: str(x) + "개")
        for col in res.columns[1:-1]:
            res[col] = res[col].apply(lambda x: str(round(x / 1e8, 2)) + "억")
        return res

    this = _agg(df, date_id)
    this_data = _process_columns(this)
    this_data = this_data.to_dict(orient="records")

    # 전일과 비교
    df = _prepare_dataframe(fname=f"sales_{month}.csv", date_id=date_id)
    df = process_sales_column(df)
    prev = _agg(df, prev_date_id)
    merged = this.merge(prev, how="left", on="단지명")

    merged["평균"] = round((merged["평균_x"] - merged["평균_y"]) / 1e8, 2)
    merged["중앙"] = round((merged["중앙_x"] - merged["중앙_y"]) / 1e8, 2)
    merged["최대"] = round((merged["최대_x"] - merged["최대_y"]) / 1e8, 2)
    merged["최저"] = round((merged["최저_x"] - merged["최저_y"]) / 1e8, 2)
    merged["매물수"] = merged["매물수_x"] - merged["매물수_y"]

    merged = merged[["평균", "중앙", "최대", "최저", "매물수"]]
    merged = _process_columns(merged)
    merged_data = merged.to_dict(orient="records")

    message = Template(TelegramTemplate.SALES_STATUS).render(
        this_data=this_data, merged_data=merged_data, zip=zip
    )

    return message


if __name__ == "__main__":
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    date_id = datetime.now().strftime("%Y-%m-%d")

    monthly_chat_id = load_env(
        "TELEGRAM_MONTHLY_CHAT_ID", ".env", start_path=PathDictionary.root
    )
    detail_chat_id = load_env(
        "TELEGRAM_DETAIL_CHAT_ID", ".env", start_path=PathDictionary.root
    )
    test_chat_id = load_env(
        "TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathDictionary.root
    )
    mode = "prod"
    block = False if mode == "test" else True

    sgg_contains = FilterDictionary.sgg_contains
    apt_contains = FilterDictionary.apt_contains

    # 월별 계약 현황
    for month in [last_month, this_month]:
        task_id = get_task_id(__file__, month, "monthly")
        msg = daily_aggregation(month, date_id=date_id, sgg_contains=sgg_contains)
        chat_id = test_chat_id if mode == "test" else monthly_chat_id

        bm = BatchManager(task_id=task_id, if_message=True, block=block)
        bm(func=send_message, text=msg, chat_id=chat_id)

    # 신규 거래
    for month in [last_month, this_month]:
        task_id = get_task_id(__file__, month, "daily_status")
        msg = daily_specific_apt(
            month, date_id=date_id, apt_contains=apt_contains, filter_new=True
        )
        chat_id = test_chat_id if mode == "test" else detail_chat_id

        bm = BatchManager(task_id=task_id, if_message=True, block=block)
        bm(func=send_message, text=msg, chat_id=chat_id)

    # 매물 집계
    task_id = get_task_id(__file__, this_month, "sales_monthly")
    msg = sales_aggregation(month=this_month, date_id=date_id)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, if_message=True, block=block)
    bm(func=send_message, text=msg, chat_id=chat_id)
