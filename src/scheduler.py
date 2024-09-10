import os
from functools import partial
from concurrent.futures import ThreadPoolExecutor
from utils import get, get_lawd_cd, parse_xml
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta
from tqdm import tqdm


def _sub_task(lawd_cd, deal_ymd):
    print(lawd_cd)
    sentinel = get(LAWD_CD=lawd_cd, DEAL_YMD=deal_ymd, pageNo=1, numOfRows=1)
    soup = BeautifulSoup(sentinel.text, 'xml')
    total_cnt = int(soup.totalCount.get_text())  # 전체 건수
    iteration = (total_cnt // 1000) + 1  # 1000 row마다 request할 때 iteration 수

    if total_cnt > 0:
        for i in range(1, iteration + 1):
            print("here")
            response = get(LAWD_CD=lawd_cd, DEAL_YMD=deal_ymd, pageNo=i, numOfRows=1000)
            if i == 1:
                result_df = parse_xml(response.text)
            else:
                result_df = pd.concat([result_df, parse_xml(response.text)])
    else:
        result_df = None

    logger.info(f"{deal_ymd} : {lawd_cd} COMPLETE")
    return result_df

def main():
    this_month = datetime.now().strftime("%Y%m")
    last_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
    date_id = datetime.now().strftime("%Y-%m-%d")

    lawd_cd_list = get_lawd_cd()

    logger.info(f"{this_month} Task Start")
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        this_month_result = list(tqdm(p.map(partial(_sub_task, deal_ymd=this_month), lawd_cd_list), total=len(lawd_cd_list)))
    this_month_result = [ele for ele in this_month_result if ele is not None]
    this_month_result = pd.concat(this_month_result)
    this_month_result['date_id'] = date_id

    logger.info(f"{last_month} Task Start")
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        last_month_result = list(tqdm(p.map(partial(_sub_task, deal_ymd=last_month), lawd_cd_list), total=len(lawd_cd_list)))
    last_month_result = [ele for ele in last_month_result if ele is not None]
    last_month_result = pd.concat(last_month_result)
    last_month_result['date_id'] = date_id

    logger.info(f"Save the data")
    os.makedirs(f"./data/{date_id}", exist_ok=True)
    this_month_result.to_csv(f"./data/{date_id}/{this_month}.csv")
    this_month_result.to_csv(f"./data/{date_id}/{last_month}.csv")

if __name__ == "__main__":
    main()
