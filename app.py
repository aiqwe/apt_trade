import streamlit as st

from src.utils import utils
import pandas as pd
from functools import lru_cache

st.set_page_config(layout="wide")


@lru_cache
def get_data(fname):
    fpath = utils.find_file(fname)
    df = pd.read_csv(fpath)
    return df


df_08 = get_data("202408.csv")
df_09 = get_data("202409.csv")

st.markdown("# 24년 8월")
st.dataframe(data=df_08)

st.markdown("# 24년 9월")
st.dataframe(data=df_09)
