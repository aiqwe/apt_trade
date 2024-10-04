import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from utils import (
    get_public_api_data,
    get_lawd_cd,
    parse_xml,
    get_task_id,
    BatchManager,
    ColumnConfig,
    PathConfig,
    SchemaConfig,
    convert_trade_columns,
    process_trade_columns,
    generate_new_trade_columns,
)


def _sub_task(lawd_cd, deal_ymd):
    # API Parameters
    # ServiceKey
    # LAWD_CD
    # DEAL_YMD
    # pageNo
    # numOfRows
    sentinel = get_public_api_data(
        url_key="분양권실거래",
        LAWD_CD=lawd_cd,
        DEAL_YMD=deal_ymd,
        pageNo=1,
        numOfRows=1,
    )

    soup = BeautifulSoup(sentinel.text, "xml")
    total_cnt = int(soup.totalCount.get_text())  # 전체 건수
    iteration = (total_cnt // 1000) + 1  # 1000 row마다 request할 때 iteration 수

    if total_cnt > 0:
        for i in range(1, iteration + 1):
            response = get_public_api_data(
                url_key="분양권실거래",
                LAWD_CD=lawd_cd,
                DEAL_YMD=deal_ymd,
                pageNo=i,
                numOfRows=1000,
            )
            if i == 1:
                result_df = parse_xml(response.text, "items")
            else:
                result_df = pd.concat([result_df, parse_xml(response.text, "items")])
    else:
        result_df = None

    logger.info(f"{deal_ymd} : {lawd_cd} COMPLETE")
    return result_df


def main_task(month: int, date_id: str):
    """
    Threadpool로 돌릴 Main Task 함수
    Args:
        month: 연월, yyyyMM 포맷이어야 하며 int 타입이어야함
        date_id: yyyy-MM-dd 포맷이어야함

    Returns:

    """
    logger.info(f"BunYang: {date_id} - {month} Task Start")

    lawd_cd = get_lawd_cd()
    lawd_cd_list = lawd_cd["lawd_cd"].to_list()
    trade_type = "분양권/입주권"

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        result = list(
            tqdm(
                p.map(partial(_sub_task, deal_ymd=month), lawd_cd_list),
                total=len(lawd_cd_list),
            )
        )
    result = [ele for ele in result if ele is not None]
    if not result:
        logger.info(f"No data in {month}")
        return
    concat = pd.concat(result)
    month = str(month)
    concat["date_id"] = date_id
    concat["month_id"] = str(month)
    concat["aptDong"] = " "
    concat["buildYear"] = " "
    concat["rgstDate"] = " "
    concat["tradeGbn"] = trade_type

    # 데이터 전처리 부분
    concat = convert_trade_columns(
        ColumnConfig.TRADE_DICTIONARY,
        concat,
        include_columns=["month_id", "date_id"],
        sort=True,
    )
    concat = concat.replace(" ", np.nan)

    # 시군구코드 -> 시군구명으로 변경하기
    lawd_cd = get_lawd_cd()
    name = lawd_cd["sgg_nm"].to_list()
    code = lawd_cd["lawd_cd"].to_list()

    converter = {}
    for n, c in zip(name, code):
        converter.update({int(c): n})
    concat["시군구코드"] = concat["시군구코드"].apply(lambda x: converter[x])
    concat = process_trade_columns(concat)
    concat["건축년도"] = concat["건축년도"].fillna("미정")
    concat = generate_new_trade_columns(concat)
    logger.info("processing columns completed")

    # 스키마 일치시키기
    concat = concat[list(SchemaConfig.trade.keys())]
    concat = concat.astype(SchemaConfig.trade)

    # Parquet로 Overwrite 저장
    path = PathConfig.bunyang
    concat.to_parquet(
        path=path,
        engine="pyarrow",
        partition_cols=["month_id", "date_id"],
        existing_data_behavior="delete_matching",
    )
    logger.info(f"Save the data in '{path}/month_id={month}/date_id={date_id}'")


if __name__ == "__main__":
    this_month = datetime.now().strftime("%Y%m")
    last_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
    # 1일이면 해당 월이 아니라 직전월 데이터를 가져옴
    if datetime.now().day == 1:
        this_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
        last_month = (datetime.now() - relativedelta(months=2)).strftime("%Y%m")

    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "prod"
    block = False if mode == "test" else True

    bm = BatchManager(
        task_id=get_task_id(__file__, last_month), key=date_id, block=block
    )
    bm(task_type="execute", func=main_task, month=last_month, date_id=date_id)
    bm = BatchManager(
        task_id=get_task_id(__file__, this_month), key=date_id, block=block
    )
    bm(task_type="execute", func=main_task, month=this_month, date_id=date_id)
