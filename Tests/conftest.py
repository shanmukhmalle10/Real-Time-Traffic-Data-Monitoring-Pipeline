# =========================================================
# conftest.py
# =========================================================
# Shared fixtures for all test modules.
# Provides a Spark session that connects to the running
# Databricks cluster and pre-loads all tables as fixtures.
# =========================================================

def spark():
    """
    Returns the active Spark session on the Databricks cluster.
    Falls back to creating a local session for local testing.
    """
    try:
        # Running on Databricks — reuse existing session
        from pyspark.context import SparkContext
        sc = SparkContext.getOrCreate()
        return SparkSession(sc)
    except Exception:
        # Local testing fallback
        return (
            SparkSession.builder
            .master("local[2]")
            .appName("traffic_pipeline_tests")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )


# ── Bronze table fixtures ─────────────────────────────────

@pytest.fixture(scope="session")
def bronze_incident(spark):
    return spark.table("traffic_catalog.bronze.incident")

@pytest.fixture(scope="session")
def bronze_sensor(spark):
    return spark.table("traffic_catalog.bronze.sensor")

@pytest.fixture(scope="session")
def bronze_speed(spark):
    return spark.table("traffic_catalog.bronze.speed")

@pytest.fixture(scope="session")
def bronze_vehicle(spark):
    return spark.table("traffic_catalog.bronze.vehicle")

@pytest.fixture(scope="session")
def bronze_signal(spark):
    return spark.table("traffic_catalog.bronze.signal")


# ── Silver table fixtures ─────────────────────────────────

@pytest.fixture(scope="session")
def silver_incident(spark):
    return spark.table("traffic_catalog.silver.incident_clean")

@pytest.fixture(scope="session")
def silver_sensor(spark):
    return spark.table("traffic_catalog.silver.sensor_clean")

@pytest.fixture(scope="session")
def silver_speed(spark):
    return spark.table("traffic_catalog.silver.speed_clean")

@pytest.fixture(scope="session")
def silver_vehicle(spark):
    return spark.table("traffic_catalog.silver.vehicle_clean")

@pytest.fixture(scope="session")
def silver_signal(spark):
    return spark.table("traffic_catalog.silver.signal_clean")


# ── Gold dimension fixtures ───────────────────────────────

@pytest.fixture(scope="session")
def gold_dim_date(spark):
    return spark.table("traffic_catalog.gold.dim_date")

@pytest.fixture(scope="session")
def gold_dim_time(spark):
    return spark.table("traffic_catalog.gold.dim_time")

@pytest.fixture(scope="session")
def gold_dim_sensor(spark):
    return spark.table("traffic_catalog.gold.dim_sensor")

@pytest.fixture(scope="session")
def gold_dim_vehicle(spark):
    return spark.table("traffic_catalog.gold.dim_vehicle")

@pytest.fixture(scope="session")
def gold_dim_incident(spark):
    return spark.table("traffic_catalog.gold.dim_incident")

@pytest.fixture(scope="session")
def gold_dim_signal(spark):
    return spark.table("traffic_catalog.gold.dim_signal")


# ── Gold fact fixture ─────────────────────────────────────

@pytest.fixture(scope="session")
def gold_fact(spark):
    return spark.table("traffic_catalog.gold.fact_traffic_events")
