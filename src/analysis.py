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

# Font 찾기
font_list = fm.findSystemFonts(fontpaths=None, fontext="ttf")
font_path = [f for f in font_list if "NanumGothic.ttf" in f][0]
font_file_name = os.path.basename(font_path)
font_name = fm.FontProperties(fname=font_path, size=10).get_name()

# System Font Path의 ttf 파일을 matplotlib의 font 설정파일로 복사
config_path = os.path.dirname(mpl.matplotlib_fname())
config_font_path = os.path.join(os.path.join(config_path, "fonts"), "ttf")
shutil.copy(font_path, os.path.join(config_font_path, font_file_name))
shutil.rmtree(mpl.get_cachedir())

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
    data = pd.read_parquet(PathConfig.sales)
    data["price_range"] = data["가격"].apply(lambda x: int(x / 1e8))
    data = data[data["면적구분"] == "84"]
    data = data[
        data["확인날짜"]
        >= (datetime.strptime(date_id, "%Y-%m-%d") - timedelta(days=7)).strftime(
            "%Y-%m-%d"
        )
    ]
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
    date_id, apt_names, agg_type: Literal["mean", "median", "min", "count"]
):
    # Make Graph and save png files in PathConfig.graph
    df = pd.read_parquet(PathConfig.sales)
    data, sorted_apt_names = _sales_trend_prep(
        df=df, apt_names=apt_names, agg_type=agg_type, date_id=date_id
    )
    converted_agg_type = agg_type_converter[agg_type]  # average -> 평균

    plt.rcParams["font.family"] = "Nanum Gothic"
    fig, ax = plt.subplots()
    ax.set_title(f"아파트별 매물 추이({converted_agg_type})")
    ax.set_xlabel("날짜")
    if agg_type == "count":
        ax.set_ylabel("갯수")
    else:
        ax.set_ylabel("가격(억)")
    plt.xticks(rotation=45)
    for apt_name in sorted_apt_names:
        panel = data[data["아파트명"] == apt_name]
        ax.plot(panel["date_id"], panel[converted_agg_type])

    ax.grid()
    ax.legend(sorted_apt_names)

    graph_path = PathConfig.graph
    if not os.path.exists(graph_path):
        os.makedirs(graph_path, exist_ok=True)
    plt.savefig(os.path.join(PathConfig.graph, f"sales_trend_{agg_type}.png"))


if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "prod"
    block = False if mode == "test" else True

    apt_names = [
        "헬리오시티",
        "파크리오",
        "마포래미안푸르지오",
        "더클래시",
        "올림픽파크포레온",
    ]
    agg_type = "mean"
    bm = BatchManager(
        task_id=get_task_id(__file__, date_id, agg_type), key=date_id, block=block
    )
    bm(
        task_type="execute",
        func=sales_trend,
        date_id=date_id,
        apt_names=apt_names,
        agg_type=agg_type,
    )

    agg_type = "median"
    bm = BatchManager(
        task_id=get_task_id(__file__, date_id, agg_type), key=date_id, block=block
    )
    bm(
        task_type="execute",
        func=sales_trend,
        date_id=date_id,
        apt_names=apt_names,
        agg_type=agg_type,
    )

    agg_type = "min"
    bm = BatchManager(
        task_id=get_task_id(__file__, date_id, agg_type), key=date_id, block=block
    )
    bm(
        task_type="execute",
        func=sales_trend,
        date_id=date_id,
        apt_names=apt_names,
        agg_type=agg_type,
    )

    agg_type = "count"
    bm = BatchManager(
        task_id=get_task_id(__file__, date_id, agg_type), key=date_id, block=block
    )
    bm(
        task_type="execute",
        func=sales_trend,
        date_id=date_id,
        apt_names=apt_names,
        agg_type=agg_type,
    )
