# =========================================================
# test_gold_fact.py
# =========================================================
# Tests for fact_traffic_events:
#   - Row count and column count
#   - Null FK checks
#   - Referential integrity vs all 6 dimensions
#   - No negative measures
#   - No unexpected string columns
#   - Boolean flag consistency
# =========================================================

import pytest
from pyspark.sql import functions as F


EXPECTED_FK_STRING_COLS = {
    "event_id", "sensor_id", "vehicle_type",
    "incident_key", "signal_id", "incident_id"
}


class TestFactRowsAndColumns:

    def test_row_count_positive(self, gold_fact):
        assert gold_fact.count() > 0, "fact_traffic_events is empty"

    def test_row_count_approx(self, gold_fact):
        count = gold_fact.count()
        assert 30000 <= count <= 60000, \
            f"Unexpected row count: {count}"

    def test_column_count(self, gold_fact):
        # Expect ~49-51 columns
        col_count = len(gold_fact.columns)
        assert 45 <= col_count <= 55, \
            f"Unexpected column count: {col_count}"

    def test_required_fk_columns_exist(self, gold_fact):
        required = {"event_id", "sensor_id", "date_key", "time_key",
                    "vehicle_type", "incident_key", "signal_id"}
        missing = required - set(gold_fact.columns)
        assert not missing, f"Missing FK columns: {missing}"

    def test_required_measure_columns_exist(self, gold_fact):
        required = {
            "vehicle_count", "avg_speed", "traffic_density",
            "occupancy_rate", "congestion_score",
            "speed_limit", "min_speed", "max_speed",
            "signal_efficiency", "green_ratio", "red_ratio",
            "vehicle_flow_rate", "vehicle_delay",
            "is_outlier", "dq_score"
        }
        missing = required - set(gold_fact.columns)
        assert not missing, f"Missing measure columns: {missing}"

    def test_required_flag_columns_exist(self, gold_fact):
        required = {"has_incident", "has_speed_violation",
                    "has_signal", "is_peak_hour"}
        missing = required - set(gold_fact.columns)
        assert not missing, f"Missing flag columns: {missing}"


class TestFactNullChecks:

    def test_no_null_sensor_ids(self, gold_fact):
        nulls = gold_fact.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_no_null_date_keys(self, gold_fact):
        nulls = gold_fact.filter(F.col("date_key").isNull()).count()
        assert nulls == 0, f"Found {nulls} null date_keys"

    def test_no_null_time_keys(self, gold_fact):
        nulls = gold_fact.filter(F.col("time_key").isNull()).count()
        assert nulls == 0, f"Found {nulls} null time_keys"

    def test_no_null_event_ids(self, gold_fact):
        nulls = gold_fact.filter(F.col("event_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null event_ids"

    def test_no_null_timestamps(self, gold_fact):
        nulls = gold_fact.filter(F.col("event_timestamp").isNull()).count()
        assert nulls == 0, f"Found {nulls} null event_timestamps"


class TestFactReferentialIntegrity:

    def test_all_sensor_ids_in_dim_sensor(self, gold_fact, gold_dim_sensor):
        fact_sensors = gold_fact.select("sensor_id").distinct()
        dim_sensors  = gold_dim_sensor.select("sensor_id").distinct()
        orphans = fact_sensors.subtract(dim_sensors).count()
        assert orphans == 0, \
            f"Found {orphans} sensor_ids in fact not in dim_sensor"

    def test_all_date_keys_in_dim_date(self, gold_fact, gold_dim_date):
        fact_dates = gold_fact.select("date_key").distinct()
        dim_dates  = gold_dim_date.select("date_key").distinct()
        orphans = fact_dates.subtract(dim_dates).count()
        assert orphans == 0, \
            f"Found {orphans} date_keys in fact not in dim_date"

    def test_all_time_keys_in_dim_time(self, gold_fact, gold_dim_time):
        fact_times = gold_fact.select("time_key").distinct()
        dim_times  = gold_dim_time.select("time_key").distinct()
        orphans = fact_times.subtract(dim_times).count()
        assert orphans == 0, \
            f"Found {orphans} time_keys in fact not in dim_time"

    def test_all_vehicle_types_in_dim_vehicle(self, gold_fact, gold_dim_vehicle):
        fact_vehicles = (gold_fact
                         .filter(F.col("vehicle_type").isNotNull())
                         .select("vehicle_type").distinct())
        dim_vehicles  = gold_dim_vehicle.select("vehicle_type").distinct()
        orphans = fact_vehicles.subtract(dim_vehicles).count()
        assert orphans == 0, \
            f"Found {orphans} vehicle_types in fact not in dim_vehicle"

    def test_all_incident_keys_in_dim_incident(self, gold_fact, gold_dim_incident):
        fact_incidents = (gold_fact
                          .filter(F.col("has_incident") == True)
                          .select("incident_key").distinct())
        dim_incidents  = gold_dim_incident.select("incident_key").distinct()
        orphans = fact_incidents.subtract(dim_incidents).count()
        assert orphans == 0, \
            f"Found {orphans} incident_keys in fact not in dim_incident"

    def test_all_signal_ids_in_dim_signal(self, gold_fact, gold_dim_signal):
        fact_signals = (gold_fact
                        .filter(F.col("signal_id").isNotNull())
                        .select("signal_id").distinct())
        dim_signals  = gold_dim_signal.select("signal_id").distinct()
        orphans = fact_signals.subtract(dim_signals).count()
        assert orphans == 0, \
            f"Found {orphans} signal_ids in fact not in dim_signal"


class TestFactRangeChecks:

    def test_vehicle_count_non_negative(self, gold_fact):
        bad = gold_fact.filter(F.col("vehicle_count") < 0).count()
        assert bad == 0, f"Found {bad} negative vehicle_count values"

    def test_avg_speed_non_negative(self, gold_fact):
        bad = gold_fact.filter(F.col("avg_speed") < 0).count()
        assert bad == 0, f"Found {bad} negative avg_speed values"

    def test_congestion_score_range(self, gold_fact):
        bad = gold_fact.filter(
            (F.col("congestion_score") < 0) |
            (F.col("congestion_score") > 100)
        ).count()
        assert bad == 0, f"Found {bad} congestion_score values outside 0–100"

    def test_occupancy_rate_range(self, gold_fact):
        bad = gold_fact.filter(
            (F.col("occupancy_rate") < 0) |
            (F.col("occupancy_rate") > 1)
        ).count()
        assert bad == 0, f"Found {bad} occupancy_rate values outside 0–1"

    def test_signal_efficiency_range(self, gold_fact):
        bad = gold_fact.filter(
            (F.col("signal_efficiency").isNotNull()) &
            ((F.col("signal_efficiency") < 0) | (F.col("signal_efficiency") > 1))
        ).count()
        assert bad == 0, f"Found {bad} signal_efficiency values outside 0–1"

    def test_dq_score_non_negative(self, gold_fact):
        bad = gold_fact.filter(F.col("dq_score") < 0).count()
        assert bad == 0, f"Found {bad} negative dq_score values"


class TestFactStringColumns:

    def test_no_unexpected_string_columns(self, gold_fact):
        """Fact should only have FK strings — no descriptive strings."""
        actual_strings = {c for c, t in gold_fact.dtypes if t == "string"}
        unexpected = actual_strings - EXPECTED_FK_STRING_COLS
        assert not unexpected, \
            f"Unexpected string columns in fact: {unexpected}"

    def test_no_incident_type_column(self, gold_fact):
        """incident_type should be in dim_incident, not in fact."""
        assert "incident_type" not in gold_fact.columns, \
            "incident_type should not be in fact table — use incident_key FK"

    def test_no_time_of_day_column(self, gold_fact):
        """time_of_day should be in dim_time, not in fact."""
        assert "time_of_day" not in gold_fact.columns, \
            "time_of_day should not be in fact table — use time_key FK"

    def test_no_congestion_level_column(self, gold_fact):
        """congestion_level string should not be in fact."""
        assert "congestion_level" not in gold_fact.columns, \
            "congestion_level should not be in fact table"

    def test_no_event_weekday_column(self, gold_fact):
        """event_weekday should be in dim_date, not in fact."""
        assert "event_weekday" not in gold_fact.columns, \
            "event_weekday should not be in fact table — use date_key FK"


class TestFactFlagConsistency:

    def test_has_incident_consistent_with_incident_id(self, gold_fact):
        """has_incident=True must have non-null incident_id."""
        bad = gold_fact.filter(
            (F.col("has_incident") == True) &
            F.col("incident_id").isNull()
        ).count()
        assert bad == 0, \
            f"Found {bad} rows where has_incident=True but incident_id is null"

    def test_has_signal_consistent_with_signal_id(self, gold_fact):
        """has_signal=True must have non-null signal_id."""
        bad = gold_fact.filter(
            (F.col("has_signal") == True) &
            F.col("signal_id").isNull()
        ).count()
        assert bad == 0, \
            f"Found {bad} rows where has_signal=True but signal_id is null"

    def test_has_speed_violation_consistent_with_flag(self, gold_fact):
        """has_speed_violation must match speed_violation_flag where both non-null."""
        bad = gold_fact.filter(
            F.col("speed_violation_flag").isNotNull() &
            (F.col("has_speed_violation") != F.col("speed_violation_flag"))
        ).count()
        assert bad == 0, \
            f"Found {bad} rows where has_speed_violation != speed_violation_flag"
