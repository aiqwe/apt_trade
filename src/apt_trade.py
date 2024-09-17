import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from utils import get_api_data, get_lawd_cd, parse_xml, BASE_URL


def _sub_task(lawd_cd, deal_ymd):
    # API Parameters
    # ServiceKey
    # LAWD_CD
    # DEAL_YMD
    # pageNo
    # numOfRows
    sentinel = get_api_data(base_url=BASE_URL['apt_trade'], LAWD_CD=lawd_cd, DEAL_YMD=deal_ymd, pageNo=1, numOfRows=1)
    soup = BeautifulSoup(sentinel.text, 'xml')
    total_cnt = int(soup.totalCount.get_text())  # 전체 건수
    iteration = (total_cnt // 1000) + 1  # 1000 row마다 request할 때 iteration 수

    if total_cnt > 0:
        for i in range(1, iteration + 1):
            response = get_api_data(base_url=BASE_URL['apt_trade'], LAWD_CD=lawd_cd, DEAL_YMD=deal_ymd, pageNo=i, numOfRows=1000)
            if i == 1:
                result_df = parse_xml(response.text, 'items')
            else:
                result_df = pd.concat([result_df, parse_xml(response.text, 'items')])
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
    logger.info(f"{date_id} - {month} Task Start")

    lawd_cd_list = get_lawd_cd()
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        result = list(tqdm(p.map(partial(_sub_task, deal_ymd=month), lawd_cd_list), total=len(lawd_cd_list)))
    result = [ele for ele in result if ele is not None]
    concat = pd.concat(result)
    concat['date_id'] = date_id

    current_path = os.path.dirname(__file__)
    os.makedirs(f"{current_path}/data/{date_id}", exist_ok=True)
    concat.to_csv(f"{current_path}/data/{date_id}/{month}_apt.csv", index=False)
    logger.info(f"Save the data in '{current_path}/data/{date_id}/{month}_apt.csv'")


def run():
    this_month = int(datetime.now().strftime("%Y%m"))
    last_month = int((datetime.now() - relativedelta(months=1)).strftime("%Y%m"))
    date_id = datetime.now().strftime("%Y-%m-%d")

    main_task(this_month, date_id)
    main_task(last_month, date_id)

if __name__ == '__main__':
    run()
