from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap, split, size, when, concat, lit, abs, to_timestamp, regexp_extract
)

catalog = "smart_claims_drv"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

@dlt.table(
    name =f"{catalog}.{silver_schema}.claim",
    comment= "Cleaned claims",
    table_properties= {
        "quality": "silver"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_number": "claim_no IS NOT NULL",
    "valid_incident_hour": "hour BETWEEN 0 and 23"
})

def claim():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claims")
    return (
        df \
            .withColumn("claim_date", to_date(col("claim_date"))) \
            .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
            .withColumn("license_issue_date", to_date(col("license_issue_date"), "dd-MM-yyyy"))
            .drop("_rescued_data")
            
    )