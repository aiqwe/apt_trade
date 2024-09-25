from utils.config import PathDictionary
from utils.utils import BatchManager, get_task_id
from git import Repo
from loguru import logger
from datetime import datetime


def git_pull():
    try:
        repo = Repo(PathDictionary.root)
        repo.remote(name="origin").pull()
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    date_id = datetime.now().strftime("%Y-%m-%d")
    mode = "test"
    block = False if mode == "test" else True

    bm = BatchManager(task_id=get_task_id(__file__), key=date_id, block=block)
    bm(func=git_pull)
