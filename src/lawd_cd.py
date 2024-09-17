import os
import pandas as pd
from loguru import logger

from utils import get_api_data, get_lawd_cd, parse_xml, BASE_URL
import json

def main():
    # API Parameters
    # ServiceKey
    # LAWD_CD
    # DEAL_YMD
    # pageNo
    # numOfRows
    logger.info("API 호출...")
    response = get_api_data(base_url=BASE_URL['lawd_cd'], pageNo=1, numOfRows=1000, type='json', locatadd_nm='서울')
    data = json.loads(response.text)['StanReginCd'][1]['row']
    df = pd.DataFrame.from_records(data)
    # save
    logger.info("저장중...")
    current_path = os.path.dirname(__file__)
    df.to_csv(f"{current_path}/data/lawd_cd.csv", index=False)

if __name__ == '__main__':
    main()
