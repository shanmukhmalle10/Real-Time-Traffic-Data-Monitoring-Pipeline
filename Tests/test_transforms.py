# =========================================================
# test_transforms.py
# =========================================================
# Unit tests for individual transform functions.
# These tests create small in-memory DataFrames to validate
# the logic of each transformation in isolation —
# independent of the actual data in Delta tables.
# =========================================================

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime


# ── Helper: create small test DataFrames ─────────────────

def make_df(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


# ── Test: dedup_by_event_id ───────────────────────────────

class TestDedupByEventId:
    """
    dedup_by_event_id keeps the latest row per event_id
    by event_timestamp descending.
    """

    def test_removes_duplicates(self, spark):
        schema = StructType([
            StructField("event_id",        StringType(),    True),
            StructField("sensor_id",       StringType(),    True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("value",           IntegerType(),   True),
        ])
        rows = [
            ("EVT001", "S1", datetime(2024, 1, 1, 8, 0), 10),
            ("EVT001", "S1", datetime(2024, 1, 1, 9, 0), 20),  # latest
            ("EVT002", "S2", datetime(2024, 1, 1, 8, 0), 30),
        ]
        df = make_df(spark, rows, schema)

        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
        result = df.withColumn("rn", F.row_number().over(w)) \
                   .filter(F.col("rn") == 1).drop("rn")

        assert result.count() == 2

    def test_keeps_latest_row(self, spark):
        schema = StructType([
            StructField("event_id",        StringType(),    True),
            StructField("event_timestamp", TimestampType(), True),
            StructField("value",           IntegerType(),   True),
        ])
        rows = [
            ("EVT001", datetime(2024, 1, 1, 8, 0), 10),
            ("EVT001", datetime(2024, 1, 1, 9, 0), 20),  # latest — should be kept
        ]
        df = make_df(spark, rows, schema)

        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
        result = df.withColumn("rn", F.row_number().over(w)) \
                   .filter(F.col("rn") == 1).drop("rn")

        kept_value = result.collect()[0]["value"]
        assert kept_value == 20, f"Expected 20 (latest), got {kept_value}"

    def test_no_duplicates_in_output(self, spark):
        schema = StructType([
            StructField("event_id",        StringType(),    True),
            StructField("event_timestamp", TimestampType(), True),
        ])
        rows = [
            ("EVT001", datetime(2024, 1, 1, 8, 0)),
            ("EVT001", datetime(2024, 1, 1, 9, 0)),
            ("EVT002", datetime(2024, 1, 1, 8, 0)),
            ("EVT002", datetime(2024, 1, 1, 8, 30)),
            ("EVT003", datetime(2024, 1, 1, 7, 0)),
        ]
        df = make_df(spark, rows, schema)

        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
        result = df.withColumn("rn", F.row_number().over(w)) \
                   .filter(F.col("rn") == 1).drop("rn")

        total    = result.count()
        distinct = result.select("event_id").distinct().count()
        assert total == distinct == 3


# ── Test: time_of_day bucketing ───────────────────────────

class TestTimeOfDayBucketing:
    """
    Validate time_of_day bucket assignment for boundary hours.
    """

    def _apply_tod(self, spark, hour):
        schema = StructType([
            StructField("event_timestamp", TimestampType(), True)
        ])
        ts = datetime(2024, 1, 1, hour, 0)
        df = make_df(spark, [(ts,)], schema)
        df = df.withColumn("time_of_day",
            F.when((F.hour("event_timestamp") >= 6)  & (F.hour("event_timestamp") < 10), F.lit("MORNING_PEAK"))
             .when((F.hour("event_timestamp") >= 10) & (F.hour("event_timestamp") < 16), F.lit("MIDDAY"))
             .when((F.hour("event_timestamp") >= 16) & (F.hour("event_timestamp") < 20), F.lit("EVENING_PEAK"))
             .when((F.hour("event_timestamp") >= 20) & (F.hour("event_timestamp") < 24), F.lit("NIGHT"))
             .otherwise(F.lit("LATE_NIGHT"))
        )
        return df.collect()[0]["time_of_day"]

    def test_hour_6_is_morning_peak(self, spark):
        assert self._apply_tod(spark, 6) == "MORNING_PEAK"

    def test_hour_9_is_morning_peak(self, spark):
        assert self._apply_tod(spark, 9) == "MORNING_PEAK"

    def test_hour_10_is_midday(self, spark):
        assert self._apply_tod(spark, 10) == "MIDDAY"

    def test_hour_15_is_midday(self, spark):
        assert self._apply_tod(spark, 15) == "MIDDAY"

    def test_hour_16_is_evening_peak(self, spark):
        assert self._apply_tod(spark, 16) == "EVENING_PEAK"

    def test_hour_19_is_evening_peak(self, spark):
        assert self._apply_tod(spark, 19) == "EVENING_PEAK"

    def test_hour_20_is_night(self, spark):
        assert self._apply_tod(spark, 20) == "NIGHT"

    def test_hour_23_is_night(self, spark):
        assert self._apply_tod(spark, 23) == "NIGHT"

    def test_hour_0_is_late_night(self, spark):
        assert self._apply_tod(spark, 0) == "LATE_NIGHT"

    def test_hour_5_is_late_night(self, spark):
        assert self._apply_tod(spark, 5) == "LATE_NIGHT"


# ── Test: congestion_level bucketing ─────────────────────

class TestCongestionLevelBucketing:

    def _apply_level(self, spark, score):
        schema = StructType([StructField("congestion_score", DoubleType(), True)])
        df = make_df(spark, [(float(score),)], schema)
        df = df.withColumn("congestion_level",
            F.when(F.col("congestion_score") <  25, F.lit("LOW"))
             .when(F.col("congestion_score") <  50, F.lit("MEDIUM"))
             .when(F.col("congestion_score") <  75, F.lit("HIGH"))
             .otherwise(F.lit("CRITICAL"))
        )
        return df.collect()[0]["congestion_level"]

    def test_score_0_is_low(self, spark):
        assert self._apply_level(spark, 0) == "LOW"

    def test_score_24_is_low(self, spark):
        assert self._apply_level(spark, 24) == "LOW"

    def test_score_25_is_medium(self, spark):
        assert self._apply_level(spark, 25) == "MEDIUM"

    def test_score_49_is_medium(self, spark):
        assert self._apply_level(spark, 49) == "MEDIUM"

    def test_score_50_is_high(self, spark):
        assert self._apply_level(spark, 50) == "HIGH"

    def test_score_74_is_high(self, spark):
        assert self._apply_level(spark, 74) == "HIGH"

    def test_score_75_is_critical(self, spark):
        assert self._apply_level(spark, 75) == "CRITICAL"

    def test_score_100_is_critical(self, spark):
        assert self._apply_level(spark, 100) == "CRITICAL"


# ── Test: speed_violation_flag logic ─────────────────────

class TestSpeedViolationFlag:

    def _apply_flag(self, spark, avg_speed, speed_limit):
        schema = StructType([
            StructField("avg_speed",   DoubleType(),  True),
            StructField("speed_limit", IntegerType(), True),
        ])
        df = make_df(spark, [(float(avg_speed), speed_limit)], schema)
        df = df.withColumn("speed_violation_flag",
            F.when(F.col("avg_speed") > F.col("speed_limit"), F.lit(True))
             .otherwise(F.lit(False))
        ).withColumn("speed_excess",
            F.round(
                F.when(F.col("avg_speed") > F.col("speed_limit"),
                       F.col("avg_speed") - F.col("speed_limit"))
                 .otherwise(F.lit(0.0)), 2
            )
        )
        row = df.collect()[0]
        return row["speed_violation_flag"], row["speed_excess"]

    def test_no_violation_at_limit(self, spark):
        flag, excess = self._apply_flag(spark, 60.0, 60)
        assert flag  is False
        assert excess == 0.0

    def test_no_violation_below_limit(self, spark):
        flag, excess = self._apply_flag(spark, 50.0, 60)
        assert flag  is False
        assert excess == 0.0

    def test_violation_above_limit(self, spark):
        flag, excess = self._apply_flag(spark, 75.0, 60)
        assert flag  is True
        assert excess == 15.0

    def test_excess_calculated_correctly(self, spark):
        flag, excess = self._apply_flag(spark, 85.5, 60)
        assert flag  is True
        assert excess == 25.5


# ── Test: lane_blocked_flag logic ────────────────────────

class TestLaneBlockedFlag:

    def _apply_flag(self, spark, value):
        schema = StructType([StructField("lane_blocked", StringType(), True)])
        df = make_df(spark, [(value,)], schema)
        df = df.withColumn("lane_blocked_flag",
            F.when(F.col("lane_blocked") == "YES",  F.lit(True))
             .when(F.col("lane_blocked") == "NO",   F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType()))
        )
        return df.collect()[0]["lane_blocked_flag"]

    def test_yes_maps_to_true(self, spark):
        assert self._apply_flag(spark, "YES") is True

    def test_no_maps_to_false(self, spark):
        assert self._apply_flag(spark, "NO") is False

    def test_unknown_maps_to_none(self, spark):
        assert self._apply_flag(spark, "UNKNOWN") is None


# ── Test: severity_rank logic ─────────────────────────────

class TestSeverityRank:

    def _apply_rank(self, spark, severity):
        schema = StructType([StructField("incident_severity", StringType(), True)])
        df = make_df(spark, [(severity,)], schema)
        df = df.withColumn("severity_rank",
            F.when(F.col("incident_severity") == "LOW",    F.lit(1))
             .when(F.col("incident_severity") == "MEDIUM", F.lit(2))
             .when(F.col("incident_severity") == "HIGH",   F.lit(3))
             .otherwise(F.lit(0))
        )
        return df.collect()[0]["severity_rank"]

    def test_low_is_rank_1(self, spark):
        assert self._apply_rank(spark, "LOW") == 1

    def test_medium_is_rank_2(self, spark):
        assert self._apply_rank(spark, "MEDIUM") == 2

    def test_high_is_rank_3(self, spark):
        assert self._apply_rank(spark, "HIGH") == 3

    def test_unknown_is_rank_0(self, spark):
        assert self._apply_rank(spark, "UNKNOWN") == 0
