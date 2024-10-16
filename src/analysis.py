import pandas as pd
from typing import Literal
from utils import PathConfig, BatchManager, get_task_id
from datetime import datetime, timedelta
import shutil
import matplotlib as mpl
from matplotlib import pyplot as plt
from matplotlib import font_manager as fm
from copy import deepcopy
import os
from argparse import ArgumentParser
import numpy as np

# Font 찾기
font_list = fm.findSystemFonts(fontpaths=None, fontext="ttf")
font_path = [f for f in font_list if "NanumGothic.ttf" in f][0]
font_file_name = os.path.basename(font_path)
font_name = fm.FontProperties(fname=font_path, size=10).get_name()

# System Font Path의 ttf 파일을 matplotlib의 font 설정파일로 복사
config_path = os.path.dirname(mpl.matplotlib_fname())
config_font_path = os.path.join(os.path.join(config_path, "fonts"), "ttf")
shutil.rmtree(mpl.get_cachedir())
shutil.copy(font_path, os.path.join(config_font_path, font_file_name))

# Font Name 가져와서 추가
plt.rc("font", family=font_name)

# agg_type 변환기
agg_type_converter = {
    "mean": "평균",
    "median": "중앙",
    "min": "최저",
    "count": "매물수",
}


def _sales_trend_prep(df, apt_names, agg_type, date_id):
    data = deepcopy(df)
    data["price_range"] = data["가격"].apply(lambda x: int(x / 1e8))
    data = data[data["면적구분"] == "84"]
    data["date_id"] = data["date_id"].astype(str)
    data["기준확인일"] = (
        data["date_id"]
        .apply(lambda x: datetime.strptime(x, "%Y-%m-%d") - timedelta(days=28))
        .astype(str)
    )
    data["유효매물"] = np.where(data["확인날짜"] >= data["기준확인일"], True, False)
    data = data[data["유효매물"]]
    data = data[data["아파트명"].isin(apt_names)]

    trend = (
        data.groupby(["아파트명", "date_id"])
        .agg({"가격": ["mean", "median", "max", "min", "count"]})
        .reset_index()
    )
    trend.columns = ["아파트명", "date_id", "평균", "중앙", "최대", "최저", "매물수"]
    trend = trend.sort_values(
        ["date_id", agg_type_converter[agg_type]], ascending=[True, False]
    )
    sorted_apt_names = trend["아파트명"].drop_duplicates().to_list()

    return trend, sorted_apt_names


def sales_trend(
    date_id, apt_names, agg_type: Literal["mean", "median", "min", "count"], sales_name: Literal['sales', 'rent']
):
    # Make Graph and save png files in PathConfig.graph
    sales_map = {
        'sales': PathConfig.sales,
        'rent': PathConfig.rent
    }

    df = pd.read_parquet(sales_map[sales_name])
    data, sorted_apt_names = _sales_trend_prep(
        df=df, apt_names=apt_names, agg_type=agg_type, date_id=date_id
    )
    converted_agg_type = agg_type_converter[agg_type]  # average -> 평균
    fig, ax = plt.subplots()
    ax.set_title(f"아파트별 매물 추이({converted_agg_type})")
    ax.set_xlabel("날짜")
    if agg_type == "count":
        ax.set_ylabel("갯수")
    else:
        ax.set_ylabel("가격(억)")
    plt.xticks(rotation=90)
    for apt_name in sorted_apt_names:
        panel = data[data["아파트명"] == apt_name]
        ax.plot(panel["date_id"], panel[converted_agg_type], marker="o")

    ax.grid()
    ax.legend(sorted_apt_names)

    graph_path = PathConfig.graph
    if not os.path.exists(graph_path):
        os.makedirs(graph_path, exist_ok=True)
    plt.savefig(os.path.join(PathConfig.graph, f"{sales_name}_trend_{agg_type}.png"))


def parse():
    parser = ArgumentParser()
    parser.add_argument("--mode", default="prod", choices=["prod", "test"])
    parser.add_argument("--nonblock", default=True, action="store_false")
    return parser.parse_args()


if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    args = parse()
    mode = args.mode.lower()
    block = args.nonblock

    apt_names = [
        "헬리오시티",
        "파크리오",
        "마포래미안푸르지오",
        "더클래시",
        "올림픽파크포레온",
    ]

    sales_types = ['sales', 'rent']
    agg_types = ['mean', 'median', 'min', 'count']
    for sales_type in sales_types:
        for agg_type in agg_types:
            bm = BatchManager(
                task_id=get_task_id(__file__, date_id, f"{sales_type}_{agg_type}"), key=date_id, block=block
            )
            bm(
                task_type="execute",
                func=sales_trend,
                date_id=date_id,
                apt_names=apt_names,
                agg_type=agg_type,
                sales_name=sales_type
            )