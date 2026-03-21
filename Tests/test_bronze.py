# =========================================================
# test_bronze.py
# =========================================================
# Tests that bronze tables exist, have the right columns,
# correct types, and non-zero row counts.
# =========================================================

import pytest


# ── Expected schemas ──────────────────────────────────────

BRONZE_INCIDENT_COLS = {
    "event_id", "sensor_id", "event_timestamp", "incident_id",
    "incident_type", "incident_severity", "incident_duration",
    "vehicles_affected", "lane_blocked", "response_time"
}

BRONZE_SENSOR_COLS = {
    "event_id", "sensor_id", "event_timestamp", "location_id",
    "lane_id", "vehicle_count", "avg_speed", "traffic_density",
    "occupancy_rate", "congestion_score"
}

BRONZE_SPEED_COLS = {
    "event_id", "sensor_id", "event_timestamp", "speed_limit",
    "avg_speed", "min_speed", "max_speed", "speed_variance",
    "speed_violation_count", "speed_index"
}

BRONZE_VEHICLE_COLS = {
    "event_id", "sensor_id", "event_timestamp", "vehicle_type",
    "vehicle_count", "vehicle_flow_rate", "vehicle_density",
    "queue_length", "lane_utilization", "vehicle_delay"
}

BRONZE_SIGNAL_COLS = {
    "event_id", "sensor_id", "event_timestamp", "signal_id",
    "signal_cycle_time", "green_time", "red_time",
    "signal_wait_time", "queue_length", "signal_efficiency"
}


# ── Bronze incident ───────────────────────────────────────

class TestBronzeIncident:

    def test_table_exists(self, bronze_incident):
        assert bronze_incident is not None

    def test_row_count_positive(self, bronze_incident):
        assert bronze_incident.count() > 0

    def test_expected_columns_present(self, bronze_incident):
        actual = set(bronze_incident.columns)
        missing = BRONZE_INCIDENT_COLS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_column_count(self, bronze_incident):
        assert len(bronze_incident.columns) == 10

    def test_row_count_approx(self, bronze_incident):
        # Bronze should have ~50,200 rows
        count = bronze_incident.count()
        assert 40000 <= count <= 60000, f"Unexpected row count: {count}"


# ── Bronze sensor ─────────────────────────────────────────

class TestBronzeSensor:

    def test_table_exists(self, bronze_sensor):
        assert bronze_sensor is not None

    def test_row_count_positive(self, bronze_sensor):
        assert bronze_sensor.count() > 0

    def test_expected_columns_present(self, bronze_sensor):
        actual  = set(bronze_sensor.columns)
        missing = BRONZE_SENSOR_COLS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_column_count(self, bronze_sensor):
        assert len(bronze_sensor.columns) == 10


# ── Bronze speed ──────────────────────────────────────────

class TestBronzeSpeed:

    def test_table_exists(self, bronze_speed):
        assert bronze_speed is not None

    def test_row_count_positive(self, bronze_speed):
        assert bronze_speed.count() > 0

    def test_expected_columns_present(self, bronze_speed):
        actual  = set(bronze_speed.columns)
        missing = BRONZE_SPEED_COLS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_column_count(self, bronze_speed):
        assert len(bronze_speed.columns) == 10


# ── Bronze vehicle ────────────────────────────────────────

class TestBronzeVehicle:

    def test_table_exists(self, bronze_vehicle):
        assert bronze_vehicle is not None

    def test_row_count_positive(self, bronze_vehicle):
        assert bronze_vehicle.count() > 0

    def test_expected_columns_present(self, bronze_vehicle):
        actual  = set(bronze_vehicle.columns)
        missing = BRONZE_VEHICLE_COLS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_column_count(self, bronze_vehicle):
        assert len(bronze_vehicle.columns) == 10


# ── Bronze signal ─────────────────────────────────────────

class TestBronzeSignal:

    def test_table_exists(self, bronze_signal):
        assert bronze_signal is not None

    def test_row_count_positive(self, bronze_signal):
        assert bronze_signal.count() > 0

    def test_expected_columns_present(self, bronze_signal):
        actual  = set(bronze_signal.columns)
        missing = BRONZE_SIGNAL_COLS - actual
        assert not missing, f"Missing columns: {missing}"

    def test_column_count(self, bronze_signal):
        assert len(bronze_signal.columns) == 10


# ── Cross-table ───────────────────────────────────────────

class TestBronzeCrossTable:

    def test_all_tables_same_approximate_row_count(
        self, bronze_incident, bronze_sensor,
        bronze_speed, bronze_vehicle, bronze_signal
    ):
        """All bronze tables should have ~50,200 rows."""
        counts = {
            "incident": bronze_incident.count(),
            "sensor":   bronze_sensor.count(),
            "speed":    bronze_speed.count(),
            "vehicle":  bronze_vehicle.count(),
            "signal":   bronze_signal.count(),
        }
        for name, count in counts.items():
            assert 40000 <= count <= 60000, \
                f"{name} has unexpected count: {count}"

    def test_all_tables_have_event_id_column(
        self, bronze_incident, bronze_sensor,
        bronze_speed, bronze_vehicle, bronze_signal
    ):
        for df, name in [
            (bronze_incident, "incident"),
            (bronze_sensor,   "sensor"),
            (bronze_speed,    "speed"),
            (bronze_vehicle,  "vehicle"),
            (bronze_signal,   "signal"),
        ]:
            assert "event_id" in df.columns, \
                f"event_id missing from bronze.{name}"

    def test_all_tables_have_sensor_id_column(
        self, bronze_incident, bronze_sensor,
        bronze_speed, bronze_vehicle, bronze_signal
    ):
        for df, name in [
            (bronze_incident, "incident"),
            (bronze_sensor,   "sensor"),
            (bronze_speed,    "speed"),
            (bronze_vehicle,  "vehicle"),
            (bronze_signal,   "signal"),
        ]:
            assert "sensor_id" in df.columns, \
                f"sensor_id missing from bronze.{name}"
