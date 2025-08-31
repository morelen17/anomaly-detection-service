import random
from argparse import ArgumentParser
from datetime import datetime, timedelta
from typing import Final

from anomaly_detection_service.constants import DB_CONNECTION_STRING
from anomaly_detection_service.db_client import SQLiteDBClient


COUNTRIES: Final = ["UK", "Denmark", "Greece", "Netherlands", "France"]


def _generate_normal_ints_with_outliers(
        n: int,
        mean: int,
        stddev: int,
        min_val: int = 0,
        outlier_prob: float = 0.05,
        outlier_factor: float = 3.,
) -> list[int]:
    values = []
    for _ in range(n):
        new_stddev = stddev * outlier_factor if random.random() < outlier_prob else stddev
        val = int(round(random.gauss(mean, new_stddev)))
        val = max(val, min_val)
        values.append(val)
    return values


def main(
        connection_string: str,
        first_registration_date: datetime,
        registrations_period_days: int,
        stats_window_size_days: int,
) -> None:
    db_client = SQLiteDBClient(connection_string)
    db_client.connect()

    db_client.execute(
        query="""\
CREATE TABLE registration_events (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    reg_datetime DATE,
    country TEXT
);
""",
        params=(),
    )
    db_client.execute(
        query="""\
CREATE TABLE anomaly_thresholds (
        registration_dt DATE,
        country TEXT,
        lower_bound FLOAT,
        upper_bound FLOAT,
        registrations_cnt INT
    );
""",
        params=(),
    )

    registration_dates = [
        first_registration_date + timedelta(days=d)
        for d in range(registrations_period_days)
    ]
    data_to_insert = []
    for country in COUNTRIES:
        mean = random.randint(50, 1000)
        stddev = mean // 6
        registrations_per_day_cnt = _generate_normal_ints_with_outliers(registrations_period_days, mean, stddev)
        for registration_cnt, cur_date in zip(registrations_per_day_cnt, registration_dates):
            for _ in range(registration_cnt):
                data_to_insert.append(
                    (cur_date.strftime("%Y-%m-%d"), country)
                )

    db_client.execute_many(
        query="INSERT INTO registration_events (reg_datetime, country) VALUES (?, ?)",
        params=data_to_insert,
    )

    # SQL script for anomaly boundary calculation
    calcs_sql = """
    WITH daily_counts AS (
        SELECT reg_datetime AS registration_dt,
               country,
               COUNT(*) AS registrations_cnt
        FROM registration_events
        GROUP BY reg_datetime, country
    ),
    rolling_stats AS (
        SELECT
            registration_dt,
            country,
            registrations_cnt,
            SUM(registrations_cnt) OVER win                        AS sum_x,
            SUM(registrations_cnt * registrations_cnt) OVER win    AS sum_x2,
            COUNT(*) OVER win                                      AS n
        FROM daily_counts
        WINDOW win AS (
            PARTITION BY country
            ORDER BY registration_dt
            ROWS BETWEEN :window_size PRECEDING AND 1 PRECEDING
        )
    ), 
    daily_stats AS (
        SELECT
            registration_dt,
            country,
            registrations_cnt,
            (CAST(sum_x AS REAL) / n) AS moving_avg,
            -- sqrt( (sum((x - mu)^2)) / N )
            SQRT((sum_x2 - 2 * sum_x * (CAST(sum_x AS REAL) / n) + n * (CAST(sum_x AS REAL) / n) * (CAST(sum_x AS REAL) / n)) / n) AS moving_std
        FROM rolling_stats
    )
    SELECT registration_dt,
           country,
           MAX(moving_avg - 3 * moving_std, 0) AS lower_bound,
           moving_avg + 3 * moving_std AS upper_bound,
           registrations_cnt
    FROM daily_stats
    """
    db_client.execute(
        query=f"""\
INSERT INTO anomaly_thresholds (registration_dt, country, lower_bound, upper_bound, registrations_cnt)
{calcs_sql};
""",
        params={"window_size": stats_window_size_days},
    )

    db_client.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--first_registration_date", type=lambda val: datetime.strptime(val, "%Y-%m-%d"), default=datetime(2025, 7, 1))
    parser.add_argument("--registrations_period_days", type=int, default=30)
    parser.add_argument("--stats_window_size_days", type=int, default=7)
    parser.add_argument("--random_seed", type=int, default=None)
    args = parser.parse_args()

    if args.random_seed is not None:
        random.seed(args.random_seed)

    main(
        connection_string=DB_CONNECTION_STRING,
        first_registration_date=args.first_registration_date,
        registrations_period_days=args.registrations_period_days,
        stats_window_size_days=args.stats_window_size_days,
    )
