from utils import PathConfig
from utils import BatchManager, get_task_id

from argparse import ArgumentParser
from git import Repo
from loguru import logger
from datetime import datetime


def git_pull():
    try:
        repo = Repo(PathConfig.root)
        repo.remote(name="origin").pull()
    except Exception as e:
        logger.error(e)


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

    bm = BatchManager(task_id=get_task_id(__file__), key=date_id, block=block)
    bm(task_type="execute", func=git_pull)
