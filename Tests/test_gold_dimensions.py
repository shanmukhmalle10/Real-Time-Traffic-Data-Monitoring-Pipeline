# =========================================================
# test_gold_dimensions.py
# =========================================================

import pytest
from pyspark.sql import functions as F


class TestDimDate:

    def test_row_count(self, gold_dim_date):
        count = gold_dim_date.count()
        assert count > 0, "dim_date is empty"
        assert count <= 366, f"dim_date has {count} rows — too many for one year"

    def test_pk_no_nulls(self, gold_dim_date):
        nulls = gold_dim_date.filter(F.col("date_key").isNull()).count()
        assert nulls == 0, f"Found {nulls} null date_keys"

    def test_pk_no_duplicates(self, gold_dim_date):
        total    = gold_dim_date.count()
        distinct = gold_dim_date.select("date_key").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate date_keys"

    def test_required_columns_exist(self, gold_dim_date):
        required = {"date_key", "full_date", "year", "month", "day",
                    "weekday_name", "week_of_year", "quarter", "is_weekend"}
        missing = required - set(gold_dim_date.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_date_key_is_integer(self, gold_dim_date):
        dtype = dict(gold_dim_date.dtypes)["date_key"]
        assert dtype == "int", f"date_key should be int, got {dtype}"

    def test_is_weekend_is_boolean(self, gold_dim_date):
        dtype = dict(gold_dim_date.dtypes)["is_weekend"]
        assert dtype == "boolean", f"is_weekend should be boolean, got {dtype}"

    def test_month_range(self, gold_dim_date):
        bad = gold_dim_date.filter(
            (F.col("month") < 1) | (F.col("month") > 12)
        ).count()
        assert bad == 0, f"Found {bad} rows with month outside 1–12"

    def test_day_range(self, gold_dim_date):
        bad = gold_dim_date.filter(
            (F.col("day") < 1) | (F.col("day") > 31)
        ).count()
        assert bad == 0, f"Found {bad} rows with day outside 1–31"

    def test_quarter_range(self, gold_dim_date):
        bad = gold_dim_date.filter(
            (F.col("quarter") < 1) | (F.col("quarter") > 4)
        ).count()
        assert bad == 0, f"Found {bad} rows with quarter outside 1–4"


class TestDimTime:

    def test_row_count(self, gold_dim_time):
        count = gold_dim_time.count()
        assert count == 1440, \
            f"dim_time should have 1440 rows (24×60), got {count}"

    def test_pk_no_nulls(self, gold_dim_time):
        nulls = gold_dim_time.filter(F.col("time_key").isNull()).count()
        assert nulls == 0, f"Found {nulls} null time_keys"

    def test_pk_no_duplicates(self, gold_dim_time):
        total    = gold_dim_time.count()
        distinct = gold_dim_time.select("time_key").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate time_keys"

    def test_required_columns_exist(self, gold_dim_time):
        required = {"time_key", "hour", "minute", "time_label",
                    "time_of_day", "is_peak_hour", "shift"}
        missing = required - set(gold_dim_time.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_hour_range(self, gold_dim_time):
        bad = gold_dim_time.filter(
            (F.col("hour") < 0) | (F.col("hour") > 23)
        ).count()
        assert bad == 0, f"Found {bad} rows with hour outside 0–23"

    def test_minute_range(self, gold_dim_time):
        bad = gold_dim_time.filter(
            (F.col("minute") < 0) | (F.col("minute") > 59)
        ).count()
        assert bad == 0, f"Found {bad} rows with minute outside 0–59"

    def test_time_of_day_valid_values(self, gold_dim_time):
        valid = {"MORNING_PEAK", "MIDDAY", "EVENING_PEAK", "NIGHT", "LATE_NIGHT"}
        invalid = (gold_dim_time
                   .filter(~F.col("time_of_day").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid time_of_day values"

    def test_is_peak_hour_is_boolean(self, gold_dim_time):
        dtype = dict(gold_dim_time.dtypes)["is_peak_hour"]
        assert dtype == "boolean", f"is_peak_hour should be boolean, got {dtype}"

    def test_peak_hours_correct(self, gold_dim_time):
        """Hours 6–9 and 16–19 should be peak. Hour 12 should not."""
        peak_6 = gold_dim_time.filter(
            (F.col("hour") == 6) & (F.col("minute") == 0)
        ).select("is_peak_hour").collect()[0][0]
        assert peak_6 is True, "Hour 6 should be peak"

        non_peak_12 = gold_dim_time.filter(
            (F.col("hour") == 12) & (F.col("minute") == 0)
        ).select("is_peak_hour").collect()[0][0]
        assert non_peak_12 is False, "Hour 12 should not be peak"


class TestDimSensor:

    def test_row_count(self, gold_dim_sensor):
        count = gold_dim_sensor.count()
        assert 490 <= count <= 510, \
            f"dim_sensor should have ~500 rows, got {count}"

    def test_pk_no_nulls(self, gold_dim_sensor):
        nulls = gold_dim_sensor.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_pk_no_duplicates(self, gold_dim_sensor):
        total    = gold_dim_sensor.count()
        distinct = gold_dim_sensor.select("sensor_id").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate sensor_ids"

    def test_required_columns_exist(self, gold_dim_sensor):
        required = {"sensor_id", "location_id", "lane_id",
                    "sensor_type", "is_active"}
        missing = required - set(gold_dim_sensor.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_is_active_all_true(self, gold_dim_sensor):
        inactive = gold_dim_sensor.filter(F.col("is_active") == False).count()
        assert inactive == 0, f"Found {inactive} inactive sensors"


class TestDimVehicle:

    def test_row_count(self, gold_dim_vehicle):
        assert gold_dim_vehicle.count() == 4, \
            "dim_vehicle should have exactly 4 rows (CAR/TRUCK/BUS/BIKE)"

    def test_pk_no_nulls(self, gold_dim_vehicle):
        nulls = gold_dim_vehicle.filter(F.col("vehicle_type").isNull()).count()
        assert nulls == 0, f"Found {nulls} null vehicle_types"

    def test_pk_no_duplicates(self, gold_dim_vehicle):
        total    = gold_dim_vehicle.count()
        distinct = gold_dim_vehicle.select("vehicle_type").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate vehicle_types"

    def test_expected_vehicle_types(self, gold_dim_vehicle):
        expected = {"CAR", "TRUCK", "BUS", "BIKE"}
        actual   = {row["vehicle_type"] for row in gold_dim_vehicle.collect()}
        assert actual == expected, \
            f"Expected vehicle types {expected}, got {actual}"

    def test_heavy_vehicle_flag_correct(self, gold_dim_vehicle):
        """TRUCK and BUS must be heavy. CAR and BIKE must not."""
        rows = {row["vehicle_type"]: row["is_heavy_vehicle"]
                for row in gold_dim_vehicle.collect()}
        assert rows["TRUCK"] is True,  "TRUCK should be heavy"
        assert rows["BUS"]   is True,  "BUS should be heavy"
        assert rows["CAR"]   is False, "CAR should not be heavy"
        assert rows["BIKE"]  is False, "BIKE should not be heavy"


class TestDimIncident:

    def test_row_count(self, gold_dim_incident):
        assert gold_dim_incident.count() == 9, \
            "dim_incident should have 9 rows (3 types × 3 severities)"

    def test_pk_no_nulls(self, gold_dim_incident):
        nulls = gold_dim_incident.filter(F.col("incident_key").isNull()).count()
        assert nulls == 0, f"Found {nulls} null incident_keys"

    def test_pk_no_duplicates(self, gold_dim_incident):
        total    = gold_dim_incident.count()
        distinct = gold_dim_incident.select("incident_key").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate incident_keys"

    def test_severity_rank_values(self, gold_dim_incident):
        """LOW=1, MEDIUM=2, HIGH=3."""
        rows = {row["incident_severity"]: row["severity_rank"]
                for row in gold_dim_incident.collect()}
        assert rows["LOW"]    == 1, "LOW should have severity_rank=1"
        assert rows["MEDIUM"] == 2, "MEDIUM should have severity_rank=2"
        assert rows["HIGH"]   == 3, "HIGH should have severity_rank=3"

    def test_requires_immediate_response_only_for_high(self, gold_dim_incident):
        bad = gold_dim_incident.filter(
            (F.col("incident_severity") != "HIGH") &
            (F.col("requires_immediate_response") == True)
        ).count()
        assert bad == 0, \
            f"Found {bad} non-HIGH rows with requires_immediate_response=True"


class TestDimSignal:

    def test_row_count_positive(self, gold_dim_signal):
        assert gold_dim_signal.count() > 0, "dim_signal is empty"

    def test_pk_no_nulls(self, gold_dim_signal):
        nulls = gold_dim_signal.filter(F.col("signal_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null signal_ids"

    def test_pk_no_duplicates(self, gold_dim_signal):
        total    = gold_dim_signal.count()
        distinct = gold_dim_signal.select("signal_id").distinct().count()
        assert total == distinct, f"Found {total - distinct} duplicate signal_ids"

    def test_required_columns_exist(self, gold_dim_signal):
        required = {"signal_id", "sensor_id", "signal_type", "is_active"}
        missing = required - set(gold_dim_signal.columns)
        assert not missing, f"Missing columns: {missing}"
