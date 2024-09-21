import requests
import os
from bs4 import BeautifulSoup
import pandas as pd
from typing import Union
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from sqlitedict import SqliteDict
from .config import PathDictionary
from loguru import logger
import asyncio
import telegram


def load_env(key: str, fname=".env", start_path=None):
    """1) 환경변수 설정이 되었는지 검색하고, 2) 설정값이 없으면 환경변수가 정의된 파일을찾는다
    Args:
        key: 검색할 환경변수 키
        fname: 환경변수를 설정한 파일명, default ".env"
        start_path: fname을 찾을 최상위 폴더, 해당 폴더에서부터 sub folder를 재귀적으로 탐색

    """
    env_path = find_file(fname)
    env = os.getenv(key, load_dotenv(env_path))
    if not env:
        raise ValueError(f"cant find env variable '{key}'")
    return env


def get_api_data(base_url: str = None, serviceKey: str = None, **params):
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
    if not serviceKey:
        serviceKey = load_env(key="PUBLIC_DATA_API_KEY", fname=".env")

    if params:
        url = (
            base_url
            + f"?serviceKey={serviceKey}&"
            + "&".join(f"{k}={v}" for k, v in params.items())
        )
    else:
        url = base_url + f"?serviceKey={serviceKey}"

    response = requests.get(url)
    return response


def parse_xml(response: str, tag):
    """xml 데이터를 파싱해서 Pandas DataFrame으로 반환

    Args:
        response: Reponse받은 텍스트값. response.text
        tag: response의 데이터 태그
    """
    soup = BeautifulSoup(response, "xml")
    data = soup.findAll(tag)[0].decode()
    return pd.read_xml(StringIO(data))


def parse_dict(data: dict):
    """dictionary 타입의 데이터를 pandas DataFrame으로 변환

    Args:
        data: 변환하려는 dict값
    """
    return pd.DataFrame.from_dict(data)


def get_lawd_cd(fname: str = "lawd_cd.csv"):
    """법정동코드를 다운받아서 서울특별시 코드 5자리만 파싱
    https://www.code.go.kr/stdcode/regCodeL.do 에서 [법정동 코드 전체 자료] 다운로드 후 사용

    Args:
        path: 법정동 코드 파일의 path
    Returns: [시군구명, 법정동코드 5자리]로 파싱된 Pandas DataFrame
    """
    path = find_file(fname)

    # int로 추론됨, str로 변경 필요
    df = pd.read_csv(path).astype(str)
    # umd_cd, ri_cd가 0으로 되어야 구 레벨의 코드
    df = df[df["region_cd"] == df["sido_cd"] + df["sgg_cd"] + "00000"]
    # 도 레벨(서울특별시) 코드 제거
    df = df[df["sgg_cd"] != "000"]

    # API에 넣을 법정동 코드 파싱하여, 시군구명과 함께 저장
    df["lawd_cd"] = df["region_cd"].str[:5]
    df = df[["locallow_nm", "lawd_cd"]]
    df.columns = ["sgg_nm", "lawd_cd"]

    return df


def find_file(fname: str, start_path: str = None):
    """fname으로 된 파일을 star_path가 None일 경우, apt_trade/ 부터 재귀적으로 검색합니다.
    가장 최근에 검색된 fname 1개만을 리턴합니다.

    Args:
        fname: 찾을 파일명(확장자 포함)
        start_path: 검색을 시작할 최상위 폴더 트리

    Returns: 검색된 파일의 abspath

    """
    if not start_path:
        start_path = PathDictionary.root  # root = apt_trade
    if not os.path.exists(start_path):
        raise ValueError(f"{start_path} does not exists.")

    paths = []
    for current_path, _, file_list in os.walk(start_path):
        if fname in file_list:
            paths.append(os.path.join(current_path, fname))
    if not paths:
        raise FileExistsError(f"'{fname}' file doesn't exists.")
    if len(paths) == 1:
        return paths[0]
    if len(paths) > 1:
        return paths


def get_task_id(file_dunder: str, *args):
    """metastore에서 체크할 task_id 생성
    Args:
        file_dunder: __file__로 고정
        *args: 추가로 task_id에 붙일 문자

    """
    basename = os.path.basename(file_dunder).lower().split(".")[0]
    return f"{basename}_{'_'.join(str(arg) for arg in args)}"


def batch_manager(task_id: str, key: str, func, if_message=False, *args, **kwargs):
    """metastore에 실행되었는지 확인후, 실행되지 않았으면 func을 실행

    Args:
        task_id: metastore에서 체크할 task_id, get_task_id 함수로 생성
        key: metastore의 key
        func: 실행할 함수
        if_message: telegram 메세지인 경우 True(async 사용 때문)
        *args: func에 전달될 인수
        **kwargs: func에 전달될 인수
    """
    db_path = os.path.join(PathDictionary.metastore, "metastore.sqlite")
    db = SqliteDict(db_path)
    if not db.get(key, None):
        db[key] = []
        db.commit()
    if task_id in db[key]:
        logger.info(f"{task_id} already executed.")
        return
    else:
        try:
            tasks = db[key]
            tasks.append(task_id)
            db[key] = tasks
            db.commit()
            db.close()
            if if_message:
                asyncio.run(send(text=kwargs["text"]))
            else:
                func(*args, **kwargs)
        except Exception as e:
            logger.error(repr(e))
            asyncio.run(send(repr(e)))


async def send(text: str, chat_id: str = None, token: str = None):
    """telegram chat_id로 메세지 전송
    Args:
        text: 전송할 메세지
        chat_id: telegram channel id
        token: bot의 token
    """
    if not token:
        token = load_env("TELEGRAM_BOT_TOKEN", ".env", start_path=PathDictionary.root)
    if not chat_id:
        chat_id = load_env("TELEGRAM_CHAT_ID", ".env", start_path=PathDictionary.root)
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=text)


def delete_metastore(
    dbpath: str = None, key: str = None, value: Union[list, str] = None
):
    """sqlitedict db metastore의 key의 특정 value를 삭제
    Args:
        dbpath: sqlite db의 위치
        key: 삭제할 key default to datetime.now()
        value: list 안에 삭제할 값
    """
    if not dbpath:
        dbpath = os.path.join(PathDictionary.metastore, "metastore.sqlite")
    if not key:
        key = datetime.now().strftime("%Y-%m-%d")
    if not value:
        raise ValueError("parameter 'value' should be passed")
    logger.info(f"key: {key}\nvalue: {value}")
    with SqliteDict(dbpath) as db:
        tasks = db[key]
        if isinstance(value, list):
            for v in value:
                tasks.remove(v)
        if isinstance(value, str):
            tasks.remove(value)
        db[key] = tasks
        db.commit()


def get_metastore(dbpath: str = None):
    if not dbpath:
        dbpath = os.path.join(PathDictionary.metastore, "metastore.sqlite")
    return SqliteDict(dbpath)
