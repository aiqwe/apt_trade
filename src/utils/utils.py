import os
import requests
import asyncio
from io import StringIO
from datetime import datetime
from typing import Literal
import inspect

import telegram
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from .config import PathConfig
from .metastore import Metastore


def get_funcname(stack_index: int = None):
    """전전 스택(호출한 함수)의 이름을 가져온다"""
    if not stack_index:
        stack_index = 1
    try:
        stack = inspect.stack()[stack_index]
    except:  # noqa: E722
        return "None"
    return stack.function


def load_env(key: str = None, fname=".env", start_path=None):
    """1) 환경변수 설정이 되었는지 검색하고, 2) 설정값이 없으면 환경변수가 정의된 파일을찾는다
    Args:
        key: 검색할 환경변수 키
        fname: 환경변수를 설정한 파일명, default ".env"
        start_path: fname을 찾을 최상위 폴더, 해당 root(apt_trade)에서부터 sub folder를 재귀적으로 탐색

    """

    env_path = find_file(fname, start_path=start_path)
    if isinstance(env_path, list) and len(env_path) > 1:
        raise ValueError(f"{env_path=}\ntwo files detected. please make unique file")
    load_dotenv(env_path)
    env = os.getenv(key, None)
    if not env:
        raise ValueError(f"cant find env variable '{key}'")
    return env


def parse_xml(response: str, tag):
    """xml 데이터를 파싱해서 Pandas DataFrame으로 반환

    Args:
        response: Reponse받은 텍스트값. response.text
        tag: response의 데이터 태그
    """
    soup = BeautifulSoup(response, "xml")
    data = soup.findAll(tag)[0].decode()
    return pd.read_xml(StringIO(data))


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
        start_path = PathConfig.root  # root = apt_trade
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


class BatchManager:
    """metastore에 실행되었는지 확인후, 실행되지 않았으면 func을 실행하는 데코레이터

    Args:
        task_id: metastore에서 체크할 task_id, get_task_id 함수로 생성
        key: metastore의 key
        func: 실행할 함수
        if_message: telegram 메세지인 경우 True(async 사용 때문)
    """

    def __init__(
        self,
        task_id: str,
        key: str = None,
        block=True,
    ):
        """
        Batchmanager는 Metadata를 관리하고, 이미 실행된 작업을 Skip하게함.
        초기화단계에서는 BatchManager가 스케줄링을 관리하는 Argument만 입력함
        Args:
            task_id: metastore에 저장될 task_id
            key: metastore에서 사용할 Key
            block: 스케줄링의 반복 실행을 블록킹할지 여부
        """
        if not key:
            key = datetime.now().strftime("%Y-%m-%d")
        self.key = key
        self.task_id = task_id
        self.block = block

    def execute(self, func, *args, **kwargs):
        """BatchManger에서 실행할 함수

        Args:
            func: 실행할 함수
            *args: func의 argument
            **kwargs: func의 keyward argument
        """
        func(*args, **kwargs)

    def send_message(self, text, chat_id, token):
        asyncio.run(send_message(text=text, chat_id=chat_id, token=token))

    def send_photo(self, photo, chat_id, token):
        asyncio.run(send_photo(photo=photo, chat_id=chat_id, token=token))

    def send_log(self, text, chat_id, token):
        asyncio.run(send_log(text=text, chat_id=chat_id, token=token))

    def __call__(
        self,
        task_type: Literal["message", "photo", "execute"],
        func,
        task_id=None,
        *args,
        **kwargs,
    ):
        if self.block:
            meta = Metastore()
            if not meta[self.key]:
                meta.setdefault(self.key, [])
                meta.commit()
            if self.task_id in meta[self.key]:
                logger.info(f"{self.task_id} already executed.")
                return
            else:
                try:
                    meta.add(key=self.key, value=self.task_id)
                    if task_type == "message":
                        self.send_message(
                            text=kwargs.get("text", None),
                            chat_id=kwargs.get("chat_id", None),
                            token=kwargs.get("token", None),
                        )
                    if task_type == "photo":
                        self.send_photo(
                            photo=kwargs.get("photo", None),
                            chat_id=kwargs.get("chat_id", None),
                            token=kwargs.get("token", None),
                        )
                    if task_type == "execute":
                        self.execute(func, *args, **kwargs)
                except Exception as e:
                    msg = f"{self.task_id}\n:{repr(e)}"
                    logger.error(msg)
                    self.send_log(
                        text=msg,
                        chat_id=None,
                        token=kwargs.get("token", None),
                    )
        else:
            try:
                if task_type == "message":
                    self.send_message(
                        text=kwargs.get("text", None),
                        chat_id=kwargs.get("chat_id", None),
                        token=kwargs.get("token", None),
                    )
                if task_type == "photo":
                    self.send_photo(
                        photo=kwargs.get("photo", None),
                        chat_id=kwargs.get("chat_id", None),
                        token=kwargs.get("token", None),
                    )
                if task_type == "execute":
                    self.execute(func, *args, **kwargs)
            except Exception as e:
                msg = f"{self.task_id}\n:{repr(e)}"
                logger.error(msg)
                self.send_log(
                    text=msg,
                    chat_id=None,
                    token=kwargs.get("token", None),
                )


def get_chat_id(token: str):
    """bot의 getUpdates를 get함으로써 chat_id 정보를 얻어내기
    Args:
        token: Telegram Bot Token
    """
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    response = requests.get(url)
    return response


async def send_log(
    text: str,
    chat_id: str = None,
    token: str = None,
    func_name: str = None,
    stack_index: int = None,
):
    """telegram chat_id로 메세지 전송
    Args:
        text: 전송할 메세지
        chat_id: telegram channel id
        token: bot의 token
    """
    if not token:
        token = load_env("TELEGRAM_BOT_TOKEN", ".env", start_path=PathConfig.root)
    if not chat_id:
        chat_id = load_env("TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathConfig.root)
    bot = telegram.Bot(token=token)
    if not func_name:
        func_name = get_funcname(stack_index=stack_index)
    text = f"{func_name}:\n" + text
    await bot.send_message(chat_id=chat_id, text=text)


async def send_message(text: str, chat_id: str = None, token: str = None):
    """telegram chat_id로 메세지 전송
    Args:
        text: 전송할 메세지
        chat_id: telegram channel id
        token: bot의 token
    """
    if not token:
        token = load_env("TELEGRAM_BOT_TOKEN", ".env", start_path=PathConfig.root)
    if not chat_id:
        chat_id = load_env("TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathConfig.root)
    bot = telegram.Bot(token=token)
    await bot.send_message(chat_id=chat_id, text=text)


async def send_photo(photo: str, chat_id: str = None, token: str = None):
    """telegram chat_id로 메세지 전송
    Args:
        photo: 전송할 이미지의 Path
        chat_id: telegram channel id
        token: bot의 token
    """
    if not token:
        token = load_env("TELEGRAM_BOT_TOKEN", ".env", start_path=PathConfig.root)
    if not chat_id:
        chat_id = load_env("TELEGRAM_TEST_CHAT_ID", ".env", start_path=PathConfig.root)
    bot = telegram.Bot(token=token)
    await bot.send_photo(chat_id=chat_id, photo=photo)
