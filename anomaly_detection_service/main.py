from datetime import datetime

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator

from anomaly_detection_service.anomaly_algo import AnomalyCheckOutput, ThreeSigmaAnomalyAlgorithm
from anomaly_detection_service.constants import DB_CONNECTION_STRING
from anomaly_detection_service.db_client import SQLiteDBClient


class AnomalyRequest(BaseModel):
    registration_dt: str

    @field_validator("registration_dt", mode="after")
    @classmethod
    def _is_valid_dt_format(cls, value: str) -> str:
        datetime.strptime(value, "%Y-%m-%d")
        return value


app = FastAPI()


@app.post("/check_anomaly")
@app.post("/check_anomaly/{algorithm}")
def check_anomaly(
        req: AnomalyRequest,
        algorithm: str = "",
) -> AnomalyCheckOutput:
    sqlite_client = SQLiteDBClient(DB_CONNECTION_STRING)
    sqlite_client.connect()

    available_algorithms = {
        "": ThreeSigmaAnomalyAlgorithm,
    }

    try:
        algo = available_algorithms[algorithm](sqlite_client)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Algorithm {algorithm} not available")

    result = algo.check(req.registration_dt)

    if not result:
        raise HTTPException(status_code=404, detail="No data for given date")

    return result


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
