#!/usr/bin/env python3
"""
Generates small synthetic fixtures with the same schema as the real NYC TLC
yellow taxi trip data, for CI -- the real monthly Parquet files aren't
committed to this repo (see data/README.md). Two months are generated (not
one) specifically so CI can run dbt build twice -- once per month -- and
exercise the incremental model and SCD Type 2 snapshot logic for real,
not just prove the SQL compiles.

Deliberately includes a few of the exact data quality issues found in the
real data while building this project: a negative-fare row, a zero-distance
row, and one fully-duplicated row (same pattern as the real duplicate found
in the February 2024 file) -- so CI is actually testing that stg_trips'
filtering and dedup logic still works, not just that it runs.

Uses DuckDB directly (already a project dependency) rather than
pandas/pyarrow, so this has no extra dependencies beyond what dbt-duckdb
already needs.
"""
import random

import duckdb

random.seed(42)

ZONES = [
    (1, "Manhattan", "Downtown"),
    (2, "Manhattan", "Midtown"),
    (3, "Brooklyn", "Williamsburg"),
    (4, "Brooklyn", "Park Slope"),
    (5, "Queens", "Astoria"),
    (6, "Queens", "Jamaica"),
    (7, "Bronx", "Fordham"),
    (8, "Staten Island", "St. George"),
]


def make_trips(month: str, n: int, vendor_bias=None):
    """month: 'YYYY-MM'. Returns a list of trip dicts."""
    year, mon = (int(x) for x in month.split("-"))
    trips = []
    for i in range(n):
        pu = random.choice(ZONES)
        do = random.choice(ZONES)
        day = random.randint(1, 27)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        duration_min = random.randint(3, 45)
        pickup = f"{year:04d}-{mon:02d}-{day:02d} {hour:02d}:{minute:02d}:00"
        dropoff_minute_total = hour * 60 + minute + duration_min
        dropoff_hour = (dropoff_minute_total // 60) % 24
        dropoff_min = dropoff_minute_total % 60
        dropoff = f"{year:04d}-{mon:02d}-{day:02d} {dropoff_hour:02d}:{dropoff_min:02d}:00"
        fare = round(random.uniform(5, 60), 2)
        tip = round(fare * random.uniform(0, 0.25), 2)
        total = round(fare + tip + 2.5 + 0.5 + 1.0, 2)

        trips.append({
            "VendorID": random.choice([1, 2]),
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": dropoff,
            "passenger_count": random.randint(1, 4),
            "trip_distance": round(random.uniform(0.5, 15), 2),
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": pu[0],
            "DOLocationID": do[0],
            "payment_type": random.choice([1, 2]),
            "fare_amount": fare,
            "extra": 2.5,
            "mta_tax": 0.5,
            "tip_amount": tip,
            "tolls_amount": 0.0,
            "improvement_surcharge": 1.0,
            "total_amount": total,
            "congestion_surcharge": 2.5,
            "Airport_fee": 0.0,
        })

    # Inject known data-quality issues, same categories found in the real data
    bad_fare = dict(trips[0])
    bad_fare["fare_amount"] = -10.0
    trips.append(bad_fare)

    bad_distance = dict(trips[1])
    bad_distance["trip_distance"] = 0.0
    trips.append(bad_distance)

    exact_duplicate = dict(trips[2])
    trips.append(dict(exact_duplicate))
    trips.append(exact_duplicate)

    return trips


def write_month(con, month: str, n: int, out_path: str):
    trips = make_trips(month, n)
    con.execute("DROP TABLE IF EXISTS _tmp_trips")
    con.execute("""
        CREATE TABLE _tmp_trips (
            VendorID INTEGER, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP,
            passenger_count BIGINT, trip_distance DOUBLE, RatecodeID BIGINT,
            store_and_fwd_flag VARCHAR, PULocationID INTEGER, DOLocationID INTEGER,
            payment_type BIGINT, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE,
            tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE,
            total_amount DOUBLE, congestion_surcharge DOUBLE, Airport_fee DOUBLE
        )
    """)
    for t in trips:
        con.execute(
            "INSERT INTO _tmp_trips VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            list(t.values()),
        )
    con.execute(f"COPY _tmp_trips TO '{out_path}' (FORMAT PARQUET)")
    print(f"Wrote {len(trips)} rows to {out_path}")


def write_zone_lookup(con, out_path: str):
    con.execute("DROP TABLE IF EXISTS _tmp_zones")
    con.execute("CREATE TABLE _tmp_zones (LocationID INTEGER, Borough VARCHAR, Zone VARCHAR, service_zone VARCHAR)")
    for loc_id, borough, zone in ZONES:
        service_zone = "Yellow Zone" if borough == "Manhattan" else "Boro Zone"
        con.execute("INSERT INTO _tmp_zones VALUES (?,?,?,?)", [loc_id, borough, zone, service_zone])
    con.execute(f"COPY _tmp_zones TO '{out_path}' (HEADER, DELIMITER ',')")
    print(f"Wrote {len(ZONES)} zones to {out_path}")


def main():
    con = duckdb.connect()
    write_month(con, "2024-01", 300, "data/sample/sample_2024-01.parquet")
    write_month(con, "2024-02", 320, "data/sample/sample_2024-02.parquet")
    write_zone_lookup(con, "data/sample/sample_taxi_zone_lookup.csv")
    con.close()


if __name__ == "__main__":
    main()
