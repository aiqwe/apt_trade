import os
from typing import Union, Any
from datetime import datetime

from sqlitedict import SqliteDict

from .config import PathConfig


class Metastore:
    def __init__(self, dbpath: str = None):
        if not dbpath:
            dbpath = os.path.join(PathConfig.metastore, "metastore.sqlite")
        self.dbpath = dbpath

    @property
    def db(self):
        return SqliteDict(self.dbpath)

    def close(self):
        self.db.close()

    def commit(self):
        self.db.commit()

    def get_keys(self):
        return list(self.db.keys())

    def get_values(self):
        return list(self.db.values())

    def get_all(self):
        return {k: v for k, v in self.db.items()}

    def delete(self, key: str = None, value: Union[list, str] = None):
        """sqlitedict db metastore의 key의 특정 value를 삭제

        Args:
            key: 삭제할 key default to datetime.now()
            value: list 안에 삭제할 값
        """

        if not key:
            key = datetime.now().strftime("%Y-%m-%d")
        if not value:
            raise ValueError("parameter 'value' should be passed")
        with self.db as db:
            tasks = db[key]
            if isinstance(value, list):
                for v in value:
                    tasks.remove(v)
            if isinstance(value, str):
                tasks.remove(value)
            db[key] = tasks
            db.commit()

    def get(self, key: str):
        return self.db.get(key, [])

    def add(self, key: str, value: Any):
        with self.db as db:
            if isinstance(value, str):
                tmp = db.get(key, [])
                tmp.append(value)
            if isinstance(value, list):
                tmp = db.setdefault(key, [])
                tmp = tmp + value
            if isinstance(value, dict):
                tmp = db.setdefault(key, {})
                tmp.update(value)
            db[key] = tmp
            db.commit()

    def setdefault(self, key: str, value: Any):
        with self.db as db:
            db.setdefault(key, value)
            db.commit()

    def __len__(self):
        return len(self.get_keys())

    def __getitem__(self, key: str):
        return self.db.get(key, [])

    def __setitem__(self, key: str, value: Any):
        with self.db as db:
            db.setdefault(key, value)
            db.commit()

    def clear(self):
        self.db.clear()
        self.commit()
