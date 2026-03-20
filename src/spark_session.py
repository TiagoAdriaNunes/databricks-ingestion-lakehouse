"""SparkSession factory — works locally and on Databricks."""

from pyspark.sql import SparkSession


def get_spark(app_name: str = "tlc-lakehouse") -> SparkSession:
    """Return a SparkSession configured for Delta Lake.

    On Databricks the active session is reused automatically.
    Locally, configure_spark_with_delta_pip handles adding the Delta JARs
    to the classpath — this is the recommended approach for delta-spark.
    """
    try:
        # on Databricks, getActiveSession() returns the managed session
        session = SparkSession.getActiveSession()
        if session is not None:
            return session
    except Exception:
        pass

    # local: configure_spark_with_delta_pip adds the JARs to the classpath;
    # the extension + catalog configs must also be set on the builder.
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # keep shuffle partitions low for local single-node runs
        .config("spark.sql.shuffle.partitions", "8")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()
