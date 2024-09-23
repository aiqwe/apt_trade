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

from utils.utils import get_api_data, get_lawd_cd, parse_xml, batch_manager, get_task_id
from utils.config import ColumnDictionary, PathDictionary, URLDictionary
from utils.processing import convert_trade_columns, merge_dataframe, process_trade_columns


def _sub_task(lawd_cd, deal_ymd):
    # API Parameters
    # ServiceKey
    # LAWD_CD
    # DEAL_YMD
    # pageNo
    # numOfRows
    sentinel = get_api_data(
        base_url=URLDictionary.URL["apt_trade"],
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
            response = get_api_data(
                base_url=URLDictionary.URL["apt_trade"],
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
    logger.info(f"Trade: {date_id} - {month} Task Start")
    lawd_cd = get_lawd_cd()
    lawd_cd_list = lawd_cd["lawd_cd"].to_list()
    trade_type = "실거래"
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        result = list(
            tqdm(
                p.map(partial(_sub_task, deal_ymd=month), lawd_cd_list),
                total=len(lawd_cd_list),
            )
        )
    result = [ele for ele in result if ele is not None]
    concat = pd.concat(result)
    concat["date_id"] = date_id

    concat["ownershipGbn"] = " "
    concat["tradeGbn"] = trade_type
    concat = convert_trade_columns(
        ColumnDictionary.TRADE_DICTIONARY,
        concat,
        include_columns=["date_id"],
        sort=True,
    )
    concat = concat.replace(" ", np.nan)

    path = os.path.join(PathDictionary.snapshot, f"{month}.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        logger.info("Data exists. we will merge org and new dataframe")
        exists = pd.read_csv(path)
        # 이미 소싱했으면 삭제후 추가
        if len(exists[(exists["date_id"] == date_id) & (exists['거래구분'] == trade_type)]) > 0:
            logger.info(f"{date_id} exists. now removing...")
            # 오늘 거래구분만 제거
            exists = exists[~((exists['거래구분'] == trade_type) & (exists['date_id'] == date_id))]
        concat = process_trade_columns(concat)
        concat = merge_dataframe(exists, concat)
    else:
        logger.info("Data doesn't exists. we will save new dataframe only")
        concat = process_trade_columns(concat)
    concat.to_csv(f"{path}", index=False)

    logger.info(f"Save the data in '{path}'")


# TODO: history 추가
if __name__ == "__main__":
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    date_id = datetime.now().strftime("%Y-%m-%d")
    block = True

    batch_manager(
        task_id=get_task_id(__file__, this_month),
        key=date_id,
        func=main_task,
        month=last_month,
        date_id=date_id,
        block=block
    )
    batch_manager(
        task_id=get_task_id(__file__, last_month),
        key=date_id,
        func=main_task,
        month=this_month,
        date_id=date_id,
        block=block
    )
