import pandas as pd
from utils import PathConfig, FilterConfig, BatchManager, get_task_id
from datetime import datetime, timedelta
from matplotlib import pyplot as plt
import os
from loguru import logger


def sales_trend(date_id):
    df = pd.read_parquet(PathConfig.sales)
    # Make Graph and save png files in PathConfig.graph
    for apt_name in FilterConfig.apt_code.keys():
        df = pd.read_parquet(PathConfig.sales)
        df["price_range"] = df["가격"].apply(lambda x: int(x / 1e8))
        df = df[df["면적구분"] == "84"]
        df = df[
            df["확인날짜"] >= (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        ]
        df = df[df["아파트명"] == apt_name]

        trend = (
            df.groupby(["아파트명", "date_id"])
            .agg({"가격": ["mean", "median", "max", "min", "count"]})
            .reset_index()
        )
        trend.columns = [
            "아파트명",
            "date_id",
            "평균",
            "중앙",
            "최대",
            "최저",
            "매물수",
        ]

        plt.rcParams["font.family"] = "Nanum Gothic"
        fig, ax = plt.subplots()
        ax.set_title(apt_name)
        ax.set_xlabel("날짜")
        ax.set_ylabel("평균 가격(억)")
        ax.plot(trend["date_id"], trend["평균"])

        graph_path = PathConfig.graph
        if not os.path.exists(graph_path):
            os.makedirs(graph_path, exist_ok=True)
        photo_path = os.path.join(PathConfig.graph, f"{apt_name}.png")
        logger.info(f"{apt_name}: Calculate mean and save png")
        plt.savefig(photo_path)


if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "test"
    block = False if mode == "test" else True

    bm = BatchManager(task_id=get_task_id(__file__, date_id), key=date_id, block=block)
    bm(func=sales_trend, date_id=date_id)
