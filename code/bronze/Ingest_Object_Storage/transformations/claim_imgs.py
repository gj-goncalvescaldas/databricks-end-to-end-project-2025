from pyspark.sql.functions import regexp_extract

@dlt.table(
    name="claim_images",
    comment="Raw accident image training data ingested from object storage",
    table_properties={"quality": "bronze"}
)

def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "BINARYFILE")
        .load(f"/Volumes/smart_claims_drv/00_landing/claims/images")
    )