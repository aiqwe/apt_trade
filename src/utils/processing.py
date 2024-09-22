import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime, timedelta
from .utils import get_lawd_cd
from loguru import logger


def _check_same_columns(df1: pd.DataFrame, df2: pd.DataFrame):
    if sorted(df1.columns) != sorted(df2.columns):
        df1_diff = list(set(df1.columns) - set(df2.columns))
        df2_diff = list(set(df2.columns) - set(df1.columns))
        raise ValueError(
            f"Columns are different.\n{list(df1_diff)} are not in new dataframe,\n{list(df2_diff)} are not in org dataframe"
        )


def convert_trade_columns(
    dictionary: dict,
    df: pd.DataFrame = None,
    column_name: str = None,
    drop=True,
    include_columns: list = None,
    sort=True,
):
    """컬럼이름 src -> dst로 변환, column_name이 df보다 우선하여 처리된다

    Args:
        dictionary: 변환을 사용할 dictionary 테이블
        df: 변환할 데이터프레임
        column_name: 변환할 컬럼명
        drop: 변환한 컬럼을 제외하고 모두 drop한다
        include_columns: drop시 해당 파라미터로 전달된 컬럼은 drop하지 않음
        sort: dictionary의 keys의 순서에 맞게 정렬한다
    """
    if df is None and not column_name:
        raise ValueError("you should assign one of 'df' or 'column_name'")

    if column_name:
        return dictionary[column_name]
    if df is not None:
        _df = deepcopy(df)
        columns = []
        alive = []
        for col in df.columns:
            if col in dictionary:
                columns.append(dictionary[col])
                alive.append(dictionary[col])
            else:
                columns.append(col)
        _df.columns = columns
        if sort:
            alive = list(dictionary.values())
        if include_columns:
            alive = alive + include_columns
        if drop:
            _df = _df[alive]
        return _df

def process_trade_columns(df: pd.DataFrame, date_id: str = None):
    """
    1) 계약일 컬럼 추가
    2) 시군구코드 시군구명으로 변경

    Args:
        df: 전처리할 데이터프레임

    """
    if not date_id:
        date_id = df['date_id'].max()

    _df = deepcopy(df)
    _df = _df[_df['date_id'] == date_id]
    logger.info(f"date_id will be processed: {date_id}")

    # 계약일 yyyy-MM-dd 형태로 추가
    _df.insert(
        loc=1,
        column="계약시점",
        value=_df["계약년도"].astype(str) + "-" + _df["계약월"].astype(str).apply(lambda x: x.rjust(2, "0")) + "-" + _df["계약일"].astype(str).apply(lambda x: x.rjust(2, "0"))
    )
    _df = _df.drop(columns=["계약년도", "계약월", "계약일"], axis=0)
    _df = _df.rename(columns={"계약시점": "계약일"})
    logger.info("Completed generating '계약일' column.")

    # 시군구코드 -> 시군구명으로 변경하기
    lawd_cd = get_lawd_cd()
    name = lawd_cd["sgg_nm"].to_list()
    code = lawd_cd["lawd_cd"].to_list()

    converter = {}
    for n, c in zip(name, code):
        converter.update({int(c): n})
    _df["시군구코드"] = _df["시군구코드"].apply(lambda x: converter[x] if isinstance(x, int) or x.isdigit() else x)
    logger.info("Completed converting '시군구코드' column.")

    _df['신규거래'] = np.nan
    logger.info("Completed generating temporary '신규거래' column with filling in 'np.nan'")

    return _df

def generate_new_trade_columns(df: pd.DataFrame, date_id: str = None):
    """ 신규거래 데이터 생성

    Args:
        df: 컬럼을 생성할 해당 월의 dataframe
        date_id: 기준 컬럼

    Returns:

    """
    if not date_id:
        date_id = df['date_id'].max()

    _df = deepcopy(df)
    logger.info(f"date_id will be processed on: {date_id}")
    prev_date_id = (
            datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    logger.info('generating seq, pk columns')
    _df['seq'] = _df.groupby(["거래구분", "아파트명", "시군구코드", "법정동", "date_id"]).cumcount() + 1
    _df['pk'] = _df['거래구분'].str[0] + _df['아파트명'] + _df['시군구코드'] + _df['법정동'] + _df['seq'].astype(str).apply(lambda x: x.rjust(5, "0"))

    # 신규거래 만들기
    _df['신규거래'] = np.where((_df['date_id'] == date_id) & (~_df['pk'].isin(_df[_df['date_id'] == prev_date_id]['pk'])), "신규", np.nan)
    logger.info("updated '신규거래' columns")
    # seq / pk 제거
    _df = _df.drop(columns=["pk", "seq"], axis=0)
    logger.info('dropped seq, pk columns')

    return _df




def merge_dataframe(org: pd.DataFrame, new: pd.DataFrame):
    """org와 new 데이터프레임 axis=0으로 머지한다

    Args:
        org: 합쳐질 데이터프레임
        new: 합칠 데이터프레임

    """
    _check_same_columns(org, new)
    return pd.concat([org, new])


def delete_latest_history(
    org: pd.DataFrame, this_month: str, last_month: str, date_column="date_id"
):
    """this_month와 last_month가 중복되면 해당 데이터를 org에서 삭제하고 새로 get한 dataframe을 추가한다

    Args:
        org: 원본 데이터프레임
        this_month: 이번달 포맷 yyyyMM, i.e. 202408
        last_month: 다음달 포맷 yyyyMM, i.e. 202409
        date_column: 스냅샷 일자 yyyy-MM-dd 형태 i.e. 2024-09-19

    Returns:

    """
    if date_column not in org.columns:
        raise ValueError(
            f"{date_column} is not in columns of org dataframe\n{org.columns=}"
        )
    _org = deepcopy(org)
    _org = _org[~_org[date_column].str.contains(last_month)]
    _org = _org[~_org[date_column].str.contains(this_month)]
    return _org
