from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ============ SILVER (CDC collapsed to current state) ============
# dp.create_streaming_table(name="ibm_dbx_sandbox.maxm_silver.bookings")
dp.create_streaming_table(
    name="ibm_dbx_sandbox.maxm_silver.bookings",
    expect_all_or_drop={
        "valid_amount": "total_amount >= 0",
        "valid_booking": "booking_id IS NOT NULL",
    },
)

dp.create_auto_cdc_flow(
    target="ibm_dbx_sandbox.maxm_silver.bookings",
    source="ibm_dbx_sandbox.maxm_bronze.booking_updates",
    keys=["booking_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)


# ============ GOLD (aggregate for metric view) ============
@dp.materialized_view(
    name="ibm_dbx_sandbox.maxm_gold.booking_metrics",
    comment="Revenue and cancellation metrics by property and month",
)
def gold_booking_metrics():
    return (
        spark.read.table("ibm_dbx_sandbox.maxm_silver.bookings")
        .groupBy(
            F.col("property_id"),
            F.date_trunc("MONTH", F.col("check_in")).alias("month"),
        )
        .agg(
            F.count("*").alias("bookings"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancellations"),
            F.round(
                100.0 * F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)) / F.count("*"),
                2,
            ).alias("cancellation_rate"),
        )
    )