import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from loguru import logger
from datetime import datetime
from tqdm import tqdm
import json
from bs4 import BeautifulSoup
from functools import partial

from utils import (
    get_naver_sales_api_data,
    get_task_id,
    BatchManager,
    FilterConfig,
    PathConfig,
    process_sales_column,
)


def _sub_task(apt_name, sales_name):
    apt_code = FilterConfig.apt_code[apt_name]
    sales_code = FilterConfig.sales_code[sales_name]
    logger.info(f"{apt_name} START")
    sentinel = get_naver_sales_api_data(
        apt_code=apt_code, sales_code=sales_code, page=0
    )
    total_cnt = json.loads(sentinel.text)["result"]["totalCount"]

    for page_idx in range((total_cnt // 30) + 1):
        response = get_naver_sales_api_data(
            apt_code=apt_code, sales_code=sales_code, page=page_idx
        )
        soup = BeautifulSoup(response.text, "html")
        rawdata = json.loads(soup.p.text)
        data = rawdata["result"]["list"]
        rows = []

        for idx in range(len(data)):
            row = {
                "아파트명": data[idx]["representativeArticleInfo"]["complexName"],
                "동": data[idx]["representativeArticleInfo"]["dongName"],
                "거래유형": data[idx]["representativeArticleInfo"]["tradeType"],
                "면적": data[idx]["representativeArticleInfo"]["spaceInfo"][
                    "exclusiveSpace"
                ],
                "면적타입": data[idx]["representativeArticleInfo"]["spaceInfo"][
                    "exclusiveSpaceName"
                ],
                "확인날짜": data[idx]["representativeArticleInfo"]["verificationInfo"][
                    "exposureStartDate"
                ],
                "인증": data[idx]["representativeArticleInfo"]["verificationInfo"][
                    "verificationType"
                ],
                "층": data[idx]["representativeArticleInfo"]["articleDetail"][
                    "floorInfo"
                ],
                "비고": data[idx]["representativeArticleInfo"]["articleDetail"][
                    "articleFeatureDescription"
                ],
                "가격": data[idx]["representativeArticleInfo"]["priceInfo"][
                    "dealPrice"
                ],
                "가격변화": data[idx]["representativeArticleInfo"]["priceInfo"][
                    "priceChangeStatus"
                ],
                "가격히스토리": data[idx]["representativeArticleInfo"]["priceInfo"][
                    "priceChangeHistories"
                ],
            }
            rows.append(row)
        if page_idx == 0:
            result = pd.DataFrame.from_records(rows)
        else:
            result = pd.concat([result, pd.DataFrame.from_records(rows)])
        # time.sleep(random.randint(0, 5))

    return result


def main_task(apt_names: list = None, date_id=None, sales_name=None):
    """apt_contains에 포함된 매물정보를 apt2me에서 가져옴
    https://apt2.me/apt/AptSellDanji.jsp?aptCode=111515&jun_size=84 방식으로 가져옴

    Args:
        apt_contains: 아파트명, FilterDictionary.

    Returns:

    """
    logger.info(f"Sales: {date_id} Task Start")
    if not apt_names:
        apt_names = list(FilterConfig.apt_code.keys())
    else:
        apt_names = {k: v for k, v in FilterConfig.apt_code.items() if k in apt_names}
    if not date_id:
        date_id = datetime.now().strftime("%Y-%m-%d")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        result = list(
            tqdm(
                p.map(partial(_sub_task, sales_name=sales_name), apt_names),
                total=len(apt_names),
            )
        )
    result = [ele for ele in result if ele is not None]
    concat = pd.concat(result)
    concat["date_id"] = date_id
    reverse_sales_code = {v: k for k, v in FilterConfig.sales_code.items()}
    concat["거래유형"] = concat["거래유형"].apply(lambda x: reverse_sales_code[x])
    concat = process_sales_column(concat)
    logger.info("processing columns completed")

    # Parquet로 Overwrite 저장
    path = PathConfig.sales
    concat.to_parquet(
        path=path,
        engine="pyarrow",
        partition_cols=["date_id"],
        existing_data_behavior="delete_matching",
    )
    logger.info(f"Save the data in '{path}/date_id={date_id}'")


# TODO: history 추가
if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "test"
    block = False if mode == "test" else True

    bm = BatchManager(task_id=get_task_id(__file__), key=date_id, block=block)

    bm(
        task_type="execute",
        func=main_task,
        apt_names=list(FilterConfig.apt_code.keys()),
        date_id=date_id,
        sales_name="매매",
    )
