import os
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from loguru import logger
from datetime import datetime
from tqdm import tqdm
from bs4 import BeautifulSoup
from io import StringIO
from functools import partial

from utils.utils import get_task_id, get_aptme_html, BatchManager
from utils.config import PathDictionary, FilterDictionary
from utils.processing import merge_dataframe, process_apt2me_table


def _sub_task(apt_name, trade_type):
    logger.info(f"{apt_name} START")
    response = get_aptme_html(apt_name, trade_type=trade_type)
    soup = BeautifulSoup(response.text, "html")
    tables = soup.find_all("table")
    if len(tables) == 0:
        result = None
    for idx, t in enumerate(tables):
        df = pd.read_html(StringIO(str(t)), skiprows=1)[0]

        if idx == 0:
            result = process_apt2me_table(df)
        else:
            result = pd.concat([result, process_apt2me_table(df)])
        result["단지명"] = apt_name
    return result


def main_task(apt_contains: list = None, date_id=None, trade_type=None):
    """apt_contains에 포함된 매물정보를 apt2me에서 가져옴
    https://apt2.me/apt/AptSellDanji.jsp?aptCode=111515&jun_size=84 방식으로 가져옴

    Args:
        apt_contains: 아파트명, FilterDictionary.

    Returns:

    """
    logger.info(f"Sales: {date_id} Task Start")
    if not apt_contains:
        apt_contains = list(FilterDictionary.apt_code.keys())
    else:
        apt_contains = {
            k: v for k, v in FilterDictionary.apt_code.items() if k in apt_contains
        }
    if not date_id:
        date_id = datetime.now().strftime("%Y-%m-%d")
    month = datetime.strptime(date_id, "%Y-%m-%f").strftime("%Y%m")

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as p:
        result = list(
            tqdm(
                p.map(partial(_sub_task, trade_type=trade_type), apt_contains),
                total=len(apt_contains),
            )
        )
    result = [ele for ele in result if ele is not None]
    concat = pd.concat(result)
    concat["date_id"] = date_id
    concat["거래구분"] = trade_type

    path = os.path.join(PathDictionary.snapshot, f"sales_{month}.csv")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        logger.info("Data exists. we will merge org and new dataframe")
        exists = pd.read_csv(path)
        # 이미 소싱했으면 삭제후 추가
        if (
            len(
                exists[
                    (exists["date_id"] == date_id) & (exists["거래구분"] == trade_type)
                ]
            )
            > 0
        ):
            logger.info(f"{date_id} exists. now removing...")
            # 오늘 거래구분만 제거
            exists = exists[
                ~((exists["거래구분"] == trade_type) & (exists["date_id"] == date_id))
            ]
        concat = merge_dataframe(exists, concat)
    else:
        logger.info("Data doesn't exists. we will save new dataframe only")
    concat.to_csv(f"{path}", index=False)

    logger.info(f"Save the data in '{path}'")


# TODO: history 추가
if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "test"
    block = False if mode == "test" else True

    bm = BatchManager(task_id=get_task_id(__file__), key=date_id, block=block)
    bm(main_task, trade_type="아파트")
    bm(main_task, trade_type="분양권")
