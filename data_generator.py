import random
from argparse import ArgumentParser
from datetime import datetime, timedelta
from typing import Final

from anomaly_detection_service.constants import DB_CONNECTION_STRING
from anomaly_detection_service.db_client import SQLiteDBClient


COUNTRIES: Final = ["UK", "Denmark", "Greece", "Netherlands", "France"]


def main(
        connection_string: str,
        number_of_registrations: int,
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

    data_to_insert = [
        (
            (first_registration_date + timedelta(days=random.randint(0, registrations_period_days - 1))).date().strftime("%Y-%m-%d"),
            random.choice(COUNTRIES),
        )
        for _ in range(number_of_registrations)
    ]
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
    daily_stats AS (
        SELECT registration_dt,
           country,
           registrations_cnt,
           AVG(registrations_cnt) OVER (
               PARTITION BY country
               ORDER BY registration_dt
               ROWS BETWEEN :window_size PRECEDING AND CURRENT ROW
           ) AS moving_avg,
           sqrt(
               AVG(registrations_cnt * registrations_cnt) OVER (
                   PARTITION BY country
                   ORDER BY registration_dt
                   ROWS BETWEEN :window_size PRECEDING AND CURRENT ROW
               ) -
               (AVG(registrations_cnt) OVER (
                   PARTITION BY country
                   ORDER BY registration_dt
                   ROWS BETWEEN :window_size PRECEDING AND CURRENT ROW
               ) *
                AVG(registrations_cnt) OVER (
                   PARTITION BY country
                   ORDER BY registration_dt
                   ROWS BETWEEN :window_size PRECEDING AND CURRENT ROW
               ))
           ) AS moving_std
        FROM daily_counts
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
        params={"window_size": stats_window_size_days - 1},
    )

    db_client.close()


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("--number_of_registrations", type=int, default=10_000)
    parser.add_argument("--first_registration_date", type=lambda val: datetime.strptime(val, "%Y-%m-%d"), default=datetime(2025, 7, 1))
    parser.add_argument("--registrations_period_days", type=int, default=30)
    parser.add_argument("--stats_window_size_days", type=int, default=7)
    parser.add_argument("--random_seed", type=int, default=None)
    args = parser.parse_args()

    if args.random_seed is not None:
        random.seed(args.random_seed)

    main(
        connection_string=DB_CONNECTION_STRING,
        number_of_registrations=args.number_of_registrations,
        first_registration_date=args.first_registration_date,
        registrations_period_days=args.registrations_period_days,
        stats_window_size_days=args.stats_window_size_days,
    )
