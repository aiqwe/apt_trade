import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import datetime, timedelta
from loguru import logger
import re
from pathlib import Path
from typing import Literal
import os
import asyncio

from .utils import PathConfig, get_lawd_cd, send_log, get_funcname


def prepare_dataframe(
    data_type: Literal["trade", "bunyang", "sales"] = None,
    date_id: str = None,
    month_id: str = None,
):
    """df가 주어지면 해당 dataframe에서 date_id, month_id를 필터링, 아니면 전체를 불러옴

    Args:
        data_type:
        date_id:
        df:
        month_id:

    Returns:

    """
    func_name = get_funcname(stack_index=2)
    fpath = str(Path(PathConfig.snapshots).joinpath(data_type))
    if date_id or month_id:
        filters = []
        if date_id:
            filters.append(("date_id", "=", date_id))
        if month_id:
            if isinstance(month_id, str):
                month_id = int(month_id.replace("-", ""))
            filters.append(("month_id", "=", month_id))
    if filters:
        df = pd.read_parquet(fpath, engine="pyarrow", filters=filters)
    else:
        df = pd.read_parquet(fpath, engine="pyarrow")

    if len(df) == 0:
        if data_type:
            if month_id:
                fpath = os.path.join(os.path.basename(fpath), str(month_id))
            if date_id:
                fpath = os.path.join(fpath, date_id)
            error_msg = f"No data found for {fpath}"
        logger.error(f"function_name: {func_name}\n{error_msg}")
        asyncio.run(send_log(error_msg))
        return pd.DataFrame()
    return df


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
        dictionary: 변환을 사용할 ColumnConfig 테이블값
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
        date_id = df["date_id"].max()

    _df = deepcopy(df)
    _df = _df[_df["date_id"] == date_id]
    logger.info(f"date_id will be processed: {date_id}")

    # 계약일 yyyy-MM-dd 형태로 추가
    _df.insert(
        loc=1,
        column="계약시점",
        value=_df["계약년도"].astype(str)
        + "-"
        + _df["계약월"].astype(str).apply(lambda x: x.rjust(2, "0"))
        + "-"
        + _df["계약일"].astype(str).apply(lambda x: x.rjust(2, "0")),
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
    _df["시군구코드"] = _df["시군구코드"].apply(
        lambda x: converter[x] if isinstance(x, int) or x.isdigit() else x
    )
    logger.info("Completed converting '시군구코드' column.")

    _df["신규거래"] = None
    logger.info(
        "Completed generating temporary '신규거래' column with filling in 'np.nan'"
    )

    return _df


def generate_new_trade_columns(df: pd.DataFrame, date_id: str):
    """신규거래 데이터 생성

    Args:
        df: 컬럼을 생성할 해당 월의 dataframe
        date_id: 기준 컬럼

    Returns:

    """
    _df = deepcopy(df)
    logger.info(f"date_id will be processed on: {date_id}")
    prev_date_id = (
        datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    logger.info("generating seq, pk columns")
    _df["seq"] = (
        _df.groupby(
            ["거래구분", "아파트명", "시군구코드", "법정동", "date_id"]
        ).cumcount()
        + 1
    )
    _df["pk"] = (
        _df["거래구분"].str[0]
        + _df["아파트명"]
        + _df["시군구코드"]
        + _df["법정동"]
        # + _df["법정동"]
        # + _df["법정동"]
        # + _df["법정동"]
        # + _df["법정동"]
        + _df["seq"].astype(str).apply(lambda x: x.rjust(5, "0"))
    )

    cur = _df[_df["date_id"] == date_id]
    prev = _df[_df["date_id"] == prev_date_id]

    merged = pd.merge(
        left=cur, right=prev[["date_id", "pk"]], left_on="pk", right_on="pk", how="left"
    )

    merged["신규거래"] = np.where(merged["date_id_y"].isna(), "신규", None)
    logger.info("updated '신규거래' columns")
    # seq / pk 제거
    merged = merged.drop(columns=["pk", "seq", "date_id_y"], axis=0)
    merged = merged.rename(columns={"date_id_x": "date_id"})
    logger.info("dropped seq, pk columns")
    return merged


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


def process_sales_column(df):
    data = deepcopy(df)
    # 면적구분 파싱 ex) 84C -> 84
    pattern = r"\d+"
    data["면적구분"] = data["면적타입"].apply(lambda x: re.match(pattern, x)[0])
    # 단지 파싱 ex) 101동 -> 1단지
    pattern = r".*\d+.*"
    data["단지"] = data["동"].apply(
        lambda x: re.match(pattern, x)[0][0] + "단지" if re.match(pattern, x) else None
    )
    data["floor"] = data["층"].apply(lambda x: x.split("/")[0])
    data["집주인"] = np.where(data["인증"] == "OWNER", "집주인", None)
    data["가격요약"] = data["가격"].apply(lambda x: f"{x/1e8:.1f}억")

    return data


def filter_sales_column(df):
    prev_7day = (
        datetime.strptime(df["확인날짜"].max(), "%Y-%m-%d") - timedelta(days=7)
    ).strftime("%Y-%m-%d")
    data = df[df["확인날짜"] >= prev_7day]

    def _convert_number(string):
        max_length = 10
        pad = max_length - len(string)
        return int(string + "0" * pad)

    data["price"] = data["가격"].apply(
        lambda x: _convert_number(x.split("억")[0] + x.split("억")[-1].split("천")[0])
    )
    data["floor"] = data["층"].apply(lambda x: x.split("/")[0])

    return data
