import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from fsspec.implementations.local import LocalFileSystem
from dotenv import load_dotenv

BASE_URL = {
    "apt_trade": "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev",
    "lawd_cd": "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"
}

def get_api_data(base_url: str = None, serviceKey: str = None, **params):
    """ 공공데이터에서 Request 함수처리

    Args:
        serviceKey: 발급받은 인증키
        base_url: API의 엔트리포인트 URL
        params:
            LAWD_CD: 법정동코드
            DEAL_YMD: 거래 연월 YYYYMM
            pageNo: 페이지 수
            numOfRows: Row 수
    """
    if not serviceKey:
        serviceKey = os.getenv("PUBLIC_DATA_API_KEY", load_dotenv())

    if params:
        url = base_url + f"?serviceKey={serviceKey}&" + f"&".join(f"{k}={v}" for k, v in params.items())
    else:
        url = base_url + f"?serviceKey={serviceKey}"

    response = requests.get(url)
    return response

def parse_xml(response, tag):
    """ xml 데이터를 파싱해서 Pandas DataFrame으로 반환

    Args:
        response: Reponse받은 텍스트값. response.text
        tag: response의 데이터 태그
    """
    soup = BeautifulSoup(response, "xml")
    data = soup.findAll(tag)[0].decode()
    return pd.read_xml(StringIO(data))

def parse_dict(data):
    """

    Args:
        data: 변환하려는 dict값
    """
    return pd.DataFrame.from_dict(data)


def get_lawd_cd(fname: str = "lawd_cd.csv"):
    """ 법정동코드를 다운받아서 서울특별시 코드 5자리만 파싱
    https://www.code.go.kr/stdcode/regCodeL.do 에서 [법정동 코드 전체 자료] 다운로드 후 사용

    Args:
        path: 법정동 코드 파일의 path
    """
    for current_path, _, file_list in os.walk(os.getcwd()):
        if fname in file_list:
            path = os.path.join(current_path, fname)

    # int로 추론됨, str로 변경 필요
    df = pd.read_csv(path).astype(str)
    # umd_cd, ri_cd가 0으로 되어야 구 레벨의 코드
    df = df[df['region_cd'] == df['sido_cd'] + df['sgg_cd'] + '00000']
    # 도 레벨(서울특별시) 코드 제거
    df = df[df['sgg_cd'] != '000']

    return df['region_cd'].str[:5].to_list()
