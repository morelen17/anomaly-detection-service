# Anomaly Detection Service

## SQL (sqlite) script for anomaly boundary calculation
Used in [data_generator.py](data_generator.py):
```
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
```

## db generation
Check optional parameters in [data_generator.py](data_generator.py):
```bash
uv run data_generator.py
```

## docker build
```bash
docker build --platform linux/arm64 -t anomaly-detection-service:latest .
```

## docker run
```bash
docker run --rm -it -p 8000:8000 anomaly-detection-service
```

## Examples

### 1.
Request:
```json
POST 127.0.0.1:8000/check_anomaly/
{
    "registration_dt": "2025-08-01"
}
```

Response: 200 OK
```json
{
    "Denmark": {
        "is_anomaly": false,
        "registrations_cnt": 65
    },
    "France": {
        "is_anomaly": false,
        "registrations_cnt": 75
    },
    "Greece": {
        "is_anomaly": true,
        "registrations_cnt": 76
    },
    "Netherlands": {
        "is_anomaly": false,
        "registrations_cnt": 55
    },
    "UK": {
        "is_anomaly": true,
        "registrations_cnt": 69
    }
}
```

### 2.
Request:
```json
POST 127.0.0.1:8000/check_anomaly/zscore
{
    "registration_dt": "2025-08-01"
}
```

Response: 404 Not Found
```json
{
    "detail": "Algorithm zscore not available"
}
```

### 3.
Request:
```json
POST 127.0.0.1:8000/check_anomaly
{
    "registration_dt": ""
}
```

Response: 422 Unprocessable Entity
```json
{
    "detail": [
        {
            "type": "value_error",
            "loc": [
                "body",
                "registration_dt"
            ],
            "msg": "Value error, time data '' does not match format '%Y-%m-%d'",
            "input": "",
            "ctx": {
                "error": {}
            }
        }
    ]
}
```
