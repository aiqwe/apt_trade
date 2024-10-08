import pandas as pd
from utils import PathConfig, BatchManager, get_task_id
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
from matplotlib import font_manager as fm

import os

# Font 찾기
font_list = fm.findSystemFonts(fontpaths=None, fontext="ttf")
fontpath = [f for f in font_list if "NanumGothic.ttf" in f] or [
    f for f in font_list if "NanumGothic.ttc" in f
]
fontpath = fontpath[0]
print(fontpath)
font_name = fm.FontProperties(fname=fontpath, size=10).get_name()
fm.findfont("NanumGothic")
plt.rc("font", family=font_name)


def sales_trend(date_id, apt_names):
    # Make Graph and save png files in PathConfig.graph
    df = pd.read_parquet(PathConfig.sales)
    df["price_range"] = df["가격"].apply(lambda x: int(x / 1e8))
    df = df[df["면적구분"] == "84"]
    df = df[df["확인날짜"] >= (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")]
    df = df[df["아파트명"].isin(apt_names)]

    trend = (
        df.groupby(["아파트명", "date_id"])
        .agg({"가격": ["mean", "median", "max", "min", "count"]})
        .reset_index()
    )
    trend.columns = ["아파트명", "date_id", "평균", "중앙", "최대", "최저", "매물수"]
    trend = trend.sort_values(["date_id", "평균"], ascending=[True, False])
    apt_names = trend["아파트명"].drop_duplicates().to_list()

    plt.rcParams["font.family"] = "Nanum Gothic"
    fig, ax = plt.subplots()
    ax.set_title("아파트별 평균 매물 추이")
    ax.set_xlabel("날짜")
    ax.set_ylabel("가격(억)")
    plt.xticks(rotation=45)
    for apt_name in apt_names:
        panel = trend[trend["아파트명"] == apt_name]
        ax.plot(panel["date_id"], panel["평균"])

    ax.grid()
    ax.legend(apt_names)

    graph_path = PathConfig.graph
    if not os.path.exists(graph_path):
        os.makedirs(graph_path, exist_ok=True)
    plt.savefig(os.path.join(PathConfig.graph, "sales_trend.png"))


if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "test"
    block = False if mode == "test" else True

    apt_names = [
        "헬리오시티",
        "파크리오",
        "마포래미안푸르지오",
        "더클래시",
        "올림픽파크포레온",
    ]

    bm = BatchManager(task_id=get_task_id(__file__, date_id), key=date_id, block=block)
    bm(task_type="execute", func=sales_trend, date_id=date_id, apt_names=apt_names)
