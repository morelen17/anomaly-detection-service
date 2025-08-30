from abc import ABC, abstractmethod
from functools import lru_cache
from typing import TypedDict

from anomaly_detection_service.db_client import DBClient


class AnomalyCountryOutput(TypedDict):
    is_anomaly: bool
    registrations_cnt: int


AnomalyCheckOutput = dict[str, AnomalyCountryOutput]


class BaseAnomalyAlgorithm(ABC):
    def __init__(self, db_client: DBClient):
        self._db_client = db_client

    @abstractmethod
    def check(self, registration_dt: str) -> AnomalyCheckOutput:
        pass


class ThreeSigmaAnomalyAlgorithm(BaseAnomalyAlgorithm):
    @lru_cache(maxsize=128)
    def check(self, registration_dt: str) -> AnomalyCheckOutput:
        thresholds_per_country = self._get_thresholds(registration_dt)
        result = {
            country: AnomalyCountryOutput(
                is_anomaly=not (lower <= cnt <= upper),
                registrations_cnt=cnt,
            )
            for country, lower, upper, cnt in thresholds_per_country
        }
        return result

    def _get_thresholds(self, registration_dt: str):
        query = """\
SELECT country, lower_bound, upper_bound, registrations_cnt
FROM anomaly_thresholds
WHERE registration_dt = ?"""
        return self._db_client.fetchall(query, (registration_dt,))