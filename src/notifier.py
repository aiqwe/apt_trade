import os.path
import pandas as pd
from copy import deepcopy
from jinja2 import Template
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Literal
from argparse import ArgumentParser

from utils import (
    prepare_dataframe,
    send_message,
    send_photo,
    load_env,
    get_task_id,
    BatchManager,
    PathConfig,
    FilterConfig,
    SchemaConfig,
    TelegramTemplate,
)


def daily_aggregation(month: str, date_id: str, sgg_contains: list = None):
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    trade = prepare_dataframe(data_type="trade", month_id=month, date_id=date_id)
    bunyang = prepare_dataframe(data_type="bunyang", month_id=month, date_id=date_id)
    # 데이터가 없을 시 처리
    df = pd.concat([trade, bunyang])
    if len(df) == 0:
        df = pd.DataFrame(columns=list(SchemaConfig.trade.keys()))
        total = 0
    else:
        total = df["계약일"].count()

    if datetime.strptime(date_id, "%Y-%m-%d").day != 1:
        last_trade = prepare_dataframe(
            data_type="trade", month_id=month, date_id=prev_date_id
        )
        last_bunyang = prepare_dataframe(
            data_type="bunyang", month_id=month, date_id=prev_date_id
        )
        last_df = pd.concat([last_trade, last_bunyang])

        # 데이터가 없을 시 처리
        if len(last_df) == 0:
            last_df = pd.DataFrame(columns=list(SchemaConfig.trade.keys()))
            last_total = 0
        else:
            last_total = last_df["계약일"].count()
        change = total - last_total
    else:
        change = 0

    agg = (
        df[df["시군구코드"].isin(sgg_contains)]
        .groupby("시군구코드")[["계약일", "계약해지여부", "신규거래"]]
        .count()
        .reset_index()
    )

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


def daily_new_trade(
    month: str, date_id: str, apt_contains: list = None, filter_new=True
):
    trade = prepare_dataframe(data_type="trade", month_id=month, date_id=date_id)
    bunyang = prepare_dataframe(data_type="bunyang", month_id=month, date_id=date_id)
    df = pd.concat([trade, bunyang])
    # 데이터가 없을 시 처리
    if len(df) == 0:
        df = pd.DataFrame(columns=list(SchemaConfig.trade.keys()))

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
    df = df[
        df["전용면적"]
        .astype(str)
        .str.split("(")
        .apply(lambda x: x[-1])
        .str.startswith("3")
    ]  # 30평대만
    df = df.sort_values(["아파트명", "전용면적", "계약일", "층"])
    data = df[cols].to_dict(orient="records")

    message = Template(TelegramTemplate.DAILY_DIFFERENCE).render(
        month=f"{str(month)[:4]}-{str(month)[4:]}", date_id=date_id, data=data, len=len
    )
    return message


def sales_aggregation(date_id):
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    # 현재로부터 7일 전까지 확인된 것
    def _agg(df, date_id):
        data = deepcopy(df)
        day_filter = (
            datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=28)
        ).strftime("%Y-%m-%d")
        data = data[data["확인날짜"] >= day_filter]
        data = data[data["면적구분"] == "84"]

        grouped = data.groupby("아파트명").agg(
            {"가격": ["mean", "median", "max", "min", "count"]}
        )
        grouped.columns = ["평균", "중앙", "최대", "최저", "매물수"]
        grouped = grouped.reset_index()
        grouped = grouped.sort_values("평균")
        return grouped

    def _process_columns(df):
        res = deepcopy(df)
        res["매물수"] = res["매물수"].apply(lambda x: str(x) + "개")
        for col in res.columns[1:-1]:
            res[col] = res[col].apply(lambda x: f"{x / 1e8:.2f}" + "억")
        return res

    df = prepare_dataframe(data_type="sales", date_id=date_id)
    this = _agg(df, date_id)

    # 이건 현재 매물용
    this_data = _process_columns(this)
    this_data = this_data.to_dict(orient="records")

    # 전일과 비교
    prev_df = prepare_dataframe(data_type="sales", date_id=prev_date_id)
    prev = _agg(prev_df, prev_date_id)
    merged = this.merge(prev, how="left", on="아파트명")

    merged["평균"] = round((merged["평균_x"] - merged["평균_y"]) / 1e8, 3)
    merged["중앙"] = round((merged["중앙_x"] - merged["중앙_y"]) / 1e8, 3)
    merged["최대"] = round((merged["최대_x"] - merged["최대_y"]) / 1e8, 3)
    merged["최저"] = round((merged["최저_x"] - merged["최저_y"]) / 1e8, 3)
    merged["매물수"] = merged["매물수_x"] - merged["매물수_y"]

    merged = merged[["아파트명", "평균", "중앙", "최대", "최저", "매물수"]]
    merged = _process_columns(merged)
    merged_data = merged.to_dict(orient="records")

    message = Template(TelegramTemplate.SALES_STATUS).render(
        this_data=this_data, merged_data=merged_data, zip=zip
    )

    return message


def sales_trend(agg_type: Literal["mean", "median", "min", "count"]):
    return os.path.join(PathConfig.graph, f"sales_trend_{agg_type}.png")


def parse():
    parser = ArgumentParser()
    parser.add_argument("--mode", default="prod", choices=["prod", "test"])
    parser.add_argument("--nonblock", default=True, action="store_false")
    return parser.parse_args()


if __name__ == "__main__":
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    # 매월 1일은 전월꺼로
    if datetime.now().day == 1:
        this_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
        last_month = int((datetime.now() - relativedelta(months=2)).strftime("%Y%m"))

    date_id = datetime.now().strftime("%Y-%m-%d")
    args = parse()
    mode = args.mode.lower()
    block = args.nonblock

    monthly_chat_id = load_env(
        "TELEGRAM_MONTHLY_CHAT_ID", ".env", start_path=PathConfig.root
    )
    detail_chat_id = load_env(
        "TELEGRAM_DETAIL_CHAT_ID", ".env", start_path=PathConfig.root
    )
    test_chat_id = load_env("TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathConfig.root)

    sgg_contains = FilterConfig.sgg_contains
    apt_contains = FilterConfig.apt_contains

    # 월별 계약 현황
    for month in [last_month, this_month]:
        task_id = get_task_id(__file__, month, "monthly")
        msg = daily_aggregation(month, date_id=date_id, sgg_contains=sgg_contains)
        chat_id = test_chat_id if mode == "test" else monthly_chat_id

        bm = BatchManager(task_id=task_id, key=date_id, block=block)
        bm(task_type="message", func=send_message, text=msg, chat_id=chat_id)

    # 신규 거래
    for month in [last_month, this_month]:
        task_id = get_task_id(__file__, month, "daily_new_trade")
        msg = daily_new_trade(
            month, date_id=date_id, apt_contains=apt_contains, filter_new=True
        )
        chat_id = test_chat_id if mode == "test" else detail_chat_id

        bm = BatchManager(task_id=task_id, key=date_id, block=block)
        bm(task_type="message", func=send_message, text=msg, chat_id=chat_id)

    # 매물 집계
    task_id = get_task_id(__file__, this_month, "sales_aggregation")
    msg = sales_aggregation(date_id=date_id)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, key=date_id, block=block)
    bm(
        task_type="message",
        task_id=task_id,
        func=send_message,
        text=msg,
        chat_id=chat_id,
    )

    # 매물 그래프 - 평균
    agg_type = "mean"
    task_id = get_task_id(__file__, f"sales_trend_{agg_type}")
    photo = sales_trend(agg_type=agg_type)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, key=date_id, block=block)
    bm(
        task_type="photo",
        task_id=task_id,
        func=send_photo,
        photo=photo,
        chat_id=chat_id,
    )

    # 매물 그래프 - 중앙
    agg_type = "median"
    task_id = get_task_id(__file__, f"sales_trend_{agg_type}")
    photo = sales_trend(agg_type=agg_type)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, key=date_id, block=block)
    bm(
        task_type="photo",
        task_id=task_id,
        func=send_photo,
        photo=photo,
        chat_id=chat_id,
    )

    # 매물 그래프 - 최저
    agg_type = "min"
    task_id = get_task_id(__file__, f"sales_trend_{agg_type}")
    photo = sales_trend(agg_type=agg_type)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, key=date_id, block=block)
    bm(
        task_type="photo",
        task_id=task_id,
        func=send_photo,
        photo=photo,
        chat_id=chat_id,
    )

    # 매물 그래프 - 매물 수
    agg_type = "count"
    task_id = get_task_id(__file__, f"sales_trend_{agg_type}")
    photo = sales_trend(agg_type=agg_type)
    chat_id = test_chat_id if mode == "test" else monthly_chat_id

    bm = BatchManager(task_id=task_id, key=date_id, block=block)
    bm(
        task_type="photo",
        task_id=task_id,
        func=send_photo,
        photo=photo,
        chat_id=chat_id,
    )
