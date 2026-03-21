# =========================================================
# test_silver.py
# =========================================================
# Tests that silver tables are properly cleaned:
#   - No null primary keys
#   - No null sensor_ids
#   - No duplicate event_ids
#   - Range checks on numeric columns
#   - Cross-column logic checks
#   - Correct derived columns exist
#   - DQ score column present
# =========================================================

import pytest
from pyspark.sql import functions as F


class TestSilverIncident:

    def test_row_count_positive(self, silver_incident):
        assert silver_incident.count() > 0

    def test_no_null_event_ids(self, silver_incident):
        nulls = silver_incident.filter(F.col("event_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null event_ids"

    def test_no_null_sensor_ids(self, silver_incident):
        nulls = silver_incident.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_no_null_incident_ids(self, silver_incident):
        nulls = silver_incident.filter(F.col("incident_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null incident_ids"

    def test_no_duplicate_event_ids(self, silver_incident):
        total   = silver_incident.count()
        distinct = silver_incident.select("event_id").distinct().count()
        assert total == distinct, \
            f"Found {total - distinct} duplicate event_ids"

    def test_incident_duration_non_negative(self, silver_incident):
        bad = silver_incident.filter(F.col("incident_duration") < 0).count()
        assert bad == 0, f"Found {bad} negative incident_duration values"

    def test_response_time_non_negative(self, silver_incident):
        bad = silver_incident.filter(F.col("response_time") < 0).count()
        assert bad == 0, f"Found {bad} negative response_time values"

    def test_vehicles_affected_non_negative(self, silver_incident):
        bad = silver_incident.filter(F.col("vehicles_affected") < 0).count()
        assert bad == 0, f"Found {bad} negative vehicles_affected values"

    def test_incident_type_valid_values(self, silver_incident):
        valid = {"ACCIDENT", "BREAKDOWN", "ROADBLOCK"}
        invalid = (silver_incident
                   .filter(~F.col("incident_type").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid incident_type values"

    def test_incident_severity_valid_values(self, silver_incident):
        valid = {"LOW", "MEDIUM", "HIGH"}
        invalid = (silver_incident
                   .filter(~F.col("incident_severity").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid incident_severity values"

    def test_lane_blocked_valid_values(self, silver_incident):
        valid = {"YES", "NO", "UNKNOWN"}
        invalid = (silver_incident
                   .filter(~F.col("lane_blocked").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid lane_blocked values"

    def test_derived_columns_exist(self, silver_incident):
        required = {"lane_blocked_flag", "is_severe", "is_peak_hour",
                    "time_of_day", "dq_score", "is_outlier"}
        missing = required - set(silver_incident.columns)
        assert not missing, f"Missing derived columns: {missing}"

    def test_timestamp_columns_exist(self, silver_incident):
        required = {"event_year", "event_month", "event_day",
                    "event_hour", "event_minute", "event_date",
                    "event_weekday", "event_week"}
        missing = required - set(silver_incident.columns)
        assert not missing, f"Missing timestamp columns: {missing}"

    def test_no_null_timestamps(self, silver_incident):
        nulls = silver_incident.filter(F.col("event_timestamp").isNull()).count()
        assert nulls == 0, f"Found {nulls} null event_timestamps"

    def test_dq_score_non_negative(self, silver_incident):
        bad = silver_incident.filter(F.col("dq_score") < 0).count()
        assert bad == 0, f"Found {bad} negative dq_score values"


class TestSilverSensor:

    def test_row_count_positive(self, silver_sensor):
        assert silver_sensor.count() > 0

    def test_no_null_event_ids(self, silver_sensor):
        nulls = silver_sensor.filter(F.col("event_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null event_ids"

    def test_no_null_sensor_ids(self, silver_sensor):
        nulls = silver_sensor.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_no_duplicate_event_ids(self, silver_sensor):
        total    = silver_sensor.count()
        distinct = silver_sensor.select("event_id").distinct().count()
        assert total == distinct, \
            f"Found {total - distinct} duplicate event_ids"

    def test_avg_speed_range(self, silver_sensor):
        bad = silver_sensor.filter(
            (F.col("avg_speed") < 0) | (F.col("avg_speed") > 200)
        ).count()
        assert bad == 0, f"Found {bad} avg_speed values outside 0–200"

    def test_occupancy_rate_range(self, silver_sensor):
        bad = silver_sensor.filter(
            (F.col("occupancy_rate") < 0) | (F.col("occupancy_rate") > 1)
        ).count()
        assert bad == 0, f"Found {bad} occupancy_rate values outside 0–1"

    def test_congestion_score_range(self, silver_sensor):
        bad = silver_sensor.filter(
            (F.col("congestion_score") < 0) | (F.col("congestion_score") > 100)
        ).count()
        assert bad == 0, f"Found {bad} congestion_score values outside 0–100"

    def test_congestion_level_valid_values(self, silver_sensor):
        valid = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
        invalid = (silver_sensor
                   .filter(~F.col("congestion_level").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid congestion_level values"

    def test_time_of_day_valid_values(self, silver_sensor):
        valid = {"MORNING_PEAK", "MIDDAY", "EVENING_PEAK", "NIGHT", "LATE_NIGHT"}
        invalid = (silver_sensor
                   .filter(~F.col("time_of_day").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid time_of_day values"

    def test_derived_columns_exist(self, silver_sensor):
        required = {"congestion_level", "is_peak_hour", "time_of_day",
                    "dq_score", "is_outlier"}
        missing = required - set(silver_sensor.columns)
        assert not missing, f"Missing derived columns: {missing}"


class TestSilverSpeed:

    def test_row_count_positive(self, silver_speed):
        assert silver_speed.count() > 0

    def test_no_null_sensor_ids(self, silver_speed):
        nulls = silver_speed.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_speed_limit_positive(self, silver_speed):
        bad = silver_speed.filter(F.col("speed_limit") <= 0).count()
        assert bad == 0, f"Found {bad} non-positive speed_limit values"

    def test_min_avg_max_order(self, silver_speed):
        """min_speed ≤ avg_speed ≤ max_speed must always hold."""
        bad = silver_speed.filter(
            (F.col("min_speed") > F.col("avg_speed")) |
            (F.col("avg_speed") > F.col("max_speed"))
        ).count()
        assert bad == 0, f"Found {bad} rows where min > avg > max order is broken"

    def test_speed_excess_non_negative(self, silver_speed):
        bad = silver_speed.filter(F.col("speed_excess") < 0).count()
        assert bad == 0, f"Found {bad} negative speed_excess values"

    def test_violation_flag_consistent_with_excess(self, silver_speed):
        """If speed_violation_flag is True then speed_excess must be > 0."""
        bad = silver_speed.filter(
            (F.col("speed_violation_flag") == True) &
            (F.col("speed_excess") <= 0)
        ).count()
        assert bad == 0, \
            f"Found {bad} rows where flag=True but excess <= 0"

    def test_derived_columns_exist(self, silver_speed):
        required = {"speed_violation_flag", "speed_excess",
                    "is_peak_hour", "time_of_day", "dq_score", "is_outlier"}
        missing = required - set(silver_speed.columns)
        assert not missing, f"Missing derived columns: {missing}"


class TestSilverVehicle:

    def test_row_count_positive(self, silver_vehicle):
        assert silver_vehicle.count() > 0

    def test_no_null_sensor_ids(self, silver_vehicle):
        nulls = silver_vehicle.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_vehicle_type_valid_values(self, silver_vehicle):
        valid = {"CAR", "TRUCK", "BUS", "BIKE"}
        invalid = (silver_vehicle
                   .filter(~F.col("vehicle_type").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid vehicle_type values"

    def test_lane_utilization_range(self, silver_vehicle):
        bad = silver_vehicle.filter(
            (F.col("lane_utilization") < 0) | (F.col("lane_utilization") > 1)
        ).count()
        assert bad == 0, f"Found {bad} lane_utilization values outside 0–1"

    def test_vehicle_count_non_negative(self, silver_vehicle):
        bad = silver_vehicle.filter(F.col("vehicle_count") < 0).count()
        assert bad == 0, f"Found {bad} negative vehicle_count values"

    def test_is_heavy_vehicle_consistent(self, silver_vehicle):
        """TRUCK and BUS must have is_heavy_vehicle = True."""
        bad = silver_vehicle.filter(
            F.col("vehicle_type").isin("TRUCK", "BUS") &
            (F.col("is_heavy_vehicle") == False)
        ).count()
        assert bad == 0, \
            f"Found {bad} TRUCK/BUS rows where is_heavy_vehicle is False"

    def test_derived_columns_exist(self, silver_vehicle):
        required = {"is_heavy_vehicle", "is_peak_hour", "time_of_day",
                    "dq_score", "is_outlier"}
        missing = required - set(silver_vehicle.columns)
        assert not missing, f"Missing derived columns: {missing}"


class TestSilverSignal:

    def test_row_count_positive(self, silver_signal):
        assert silver_signal.count() > 0

    def test_no_null_sensor_ids(self, silver_signal):
        nulls = silver_signal.filter(F.col("sensor_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null sensor_ids"

    def test_no_null_signal_ids(self, silver_signal):
        nulls = silver_signal.filter(F.col("signal_id").isNull()).count()
        assert nulls == 0, f"Found {nulls} null signal_ids"

    def test_signal_cycle_time_positive(self, silver_signal):
        bad = silver_signal.filter(F.col("signal_cycle_time") <= 0).count()
        assert bad == 0, f"Found {bad} non-positive signal_cycle_time values"

    def test_green_red_within_cycle(self, silver_signal):
        """green_time + red_time must not exceed signal_cycle_time."""
        bad = silver_signal.filter(
            (F.col("green_time") + F.col("red_time")) > F.col("signal_cycle_time")
        ).count()
        assert bad == 0, \
            f"Found {bad} rows where green+red > cycle_time"

    def test_signal_efficiency_range(self, silver_signal):
        bad = silver_signal.filter(
            (F.col("signal_efficiency") < 0) | (F.col("signal_efficiency") > 1)
        ).count()
        assert bad == 0, \
            f"Found {bad} signal_efficiency values outside 0–1"

    def test_green_ratio_range(self, silver_signal):
        bad = silver_signal.filter(
            (F.col("green_ratio") < 0) | (F.col("green_ratio") > 1)
        ).count()
        assert bad == 0, \
            f"Found {bad} green_ratio values outside 0–1"

    def test_signal_status_valid_values(self, silver_signal):
        valid = {"GOOD", "MODERATE", "POOR"}
        invalid = (silver_signal
                   .filter(~F.col("signal_status").isin(*valid))
                   .count())
        assert invalid == 0, f"Found {invalid} invalid signal_status values"

    def test_derived_columns_exist(self, silver_signal):
        required = {"green_ratio", "red_ratio", "signal_status",
                    "is_peak_hour", "time_of_day", "dq_score", "is_outlier"}
        missing = required - set(silver_signal.columns)
        assert not missing, f"Missing derived columns: {missing}"


class TestSilverCrossTable:

    def test_all_tables_have_500_sensors(
        self, silver_incident, silver_sensor,
        silver_speed, silver_vehicle, silver_signal
    ):
        """All silver tables should have ~500 distinct sensors."""
        for df, name in [
            (silver_incident, "incident"),
            (silver_sensor,   "sensor"),
            (silver_speed,    "speed"),
            (silver_vehicle,  "vehicle"),
            (silver_signal,   "signal"),
        ]:
            count = df.select("sensor_id").distinct().count()
            assert 490 <= count <= 510, \
                f"silver.{name} has {count} distinct sensors — expected ~500"

    def test_no_orphan_sensors_incident_vs_sensor(
        self, silver_incident, silver_sensor
    ):
        """All sensor_ids in incident must exist in sensor."""
        incident_sensors = silver_incident.select("sensor_id").distinct()
        sensor_sensors   = silver_sensor.select("sensor_id").distinct()
        orphans = incident_sensors.subtract(sensor_sensors).count()
        assert orphans == 0, \
            f"Found {orphans} sensor_ids in incident not in sensor table"
