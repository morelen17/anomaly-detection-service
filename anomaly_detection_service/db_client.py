import sqlite3
from abc import ABC, abstractmethod
from typing import Optional


class DBClient(ABC):
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def execute(self, query: str, params) -> None:
        raise NotImplementedError()

    @abstractmethod
    def fetchall(self, query: str, params):
        raise NotImplementedError()


class SQLiteDBClient(DBClient):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None

    def connect(self) -> None:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()

    def close(self) -> None:
        if self.conn:
            self.conn.commit()
            self.conn.close()
            self.conn = None
            self.cursor = None

    def execute(self, query: str, params) -> None:
        if not self.cursor:
            raise RuntimeError("Database not connected")
        self.cursor.execute(query, params)
        self.conn.commit()

    def execute_many(self, query: str, params) -> None:
        if not self.cursor:
            raise RuntimeError("Database not connected")
        self.cursor.executemany(query, params)
        self.conn.commit()

    def fetchall(self, query: str, params) -> list[tuple]:
        self.execute(query, params)
        return self.cursor.fetchall()
