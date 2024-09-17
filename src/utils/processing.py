from typing import Union
import pandas as pd
from copy import deepcopy

def _check_same_columns(df1: pd.DataFrame, df2: pd.DataFrame):
    if sorted(df1.columns) != sorted(df2.columns):
        df1_diff = list(set(df1.columns) - set(df2.columns))
        df2_diff = list(set(df2.columns) - set(df1.columns))
        raise ValueError(f"Columns are different.\n{list(df1_diff)} are not in new dataframe,\n{list(df2_diff)} are not in org dataframe")

def convert_column(dictionary: dict, df: pd.DataFrame = None, column_name: str = None, drop = True, include_columns: list = None, sort = True):
    """ 컬럼이름 src -> dst로 변환, column_name이 df보다 우선하여 처리된다

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

def merge_dataframe(org: pd.DataFrame, new: pd.DataFrame):
    _check_same_columns(org, new)
    return pd.concat([org, new])

def delete_latest_history(org: pd.DataFrame, this_month: str, last_month: str, date_column = "date_id"):
    if date_column not in org.columns:
        raise ValueError(f"{date_column} is not in columns of org dataframe\n{org.columns=}")
    _org = deepcopy(org)
    _org = _org[~_org[date_column].str.contains(last_month)]
    _org = _org[~_org[date_column].str.contains(this_month)]
    return _org


