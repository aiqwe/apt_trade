import requests
import os
from bs4 import BeautifulSoup
import pandas as pd

def get(LAWD_CD: int, DEAL_YMD: int, pageNo:int, numOfRows: int, serviceKey: str = None):
    """ 공공데이터에서 Request 함수처리

    Args:
        serviceKey: 발급받은 인증키
        LAWD_CD: 법정동코드
        DEAL_YMD: 거래 연월 YYYYMM
        pageNo: 페이지 수
        numOfRows: Row 수
    """
    if not serviceKey:
        serviceKey = os.environ("PUBLIC_DATA_API_KEY")

    url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev"
    url = url + f"?serviceKey={serviceKey}&{LAWD_CD=}&{DEAL_YMD=}&{pageNo=}&{numOfRows=}"

    response = requests.get(url)
    return response

def parse_xml(response):
    """ xml 데이터를 파싱해서 Pandas DataFrame으로 반환

    Args:
        response: Reponse받은 텍스트값. response.text
    """
    soup = BeautifulSoup(response, "xml")
    return pd.read_xml(str(soup.items))

def get_lawd_cd(path: str = "LAWD_CD.txt"):
    """ 법정동코드를 다운받아서 서울특별시 코드 5자리만 파싱
    https://www.code.go.kr/stdcode/regCodeL.do 에서 [법정동 코드 전체 자료] 다운로드 후 사용

    Args:
        path: 법정동 코드 파일의 path
    """
    with open(path, "rb") as f:
        lawd_cd = f.read().decode("cp949")

    lawd_cd_list = set()
    for line in lawd_cd.splitlines():
        if ("서울특별시" in line) and (line[:5].isdigit()) and len(line[:5]) == 5:
            lawd_cd_list.add(int(line[:5]))

    return list(lawd_cd_list)
