from typing import Literal
from .utils import load_env
from .config import URLConfig
import requests

def get_public_api_data(url_key: Literal["아파트실거래", "분양권실거래"] = None, serviceKey: str = None, base_url: str = None, **kwargs):
    """공공데이터에서 Request 함수처리

    Args:
        serviceKey: 발급받은 인증키
        base_url: API의 엔트리포인트 URL
        params:
            LAWD_CD: 법정동코드
            DEAL_YMD: 거래 연월 YYYYMM
            pageNo: 페이지 수
            numOfRows: Row 수
    """
    if not url_key and not base_url:
        raise ValueError("one of 'url_key' or 'base_url' should be specified")
    if url_key:
        base_url = URLConfig.URL[url_key]
    if not serviceKey:
        serviceKey = load_env(key="PUBLIC_DATA_API_KEY", fname=".env")
    params = dict(serviceKey=serviceKey)
    params.update(kwargs)
    response = requests.get(url=base_url, params=params)
    return response

def get_naver_sales_api_data(url_key: Literal["네이버매물"] = None, base_url: str = None, headers: dict = None, **kwargs):
    """네이버 front-api에서 데이터 가져오기

    Args:
        serviceKey: 발급받은 인증키
        base_url: API의 엔트리포인트 URL
        params:
            apt_code: 아파트코드
            sales_code: 거래 타입
            page: 페이지 수
    """
    if not url_key and not base_url:
        base_url = URLConfig.URL["네이버매물"]
    if url_key:
        base_url = URLConfig.URL[url_key]
    if not headers:
        headers = {"User-Agent": URLConfig.FakeAgent}
    params = {
        "complexNumber": kwargs['apt_code'],
        "tradeTypes": kwargs['sales_code'],
        "userChannelType": "PC",
        "page": kwargs['page']
    }
    response = requests.get(url=base_url, params=params, headers=headers)
    return response