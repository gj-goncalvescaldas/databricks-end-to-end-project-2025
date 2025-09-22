from pyspark.sql.functions import regexp_extract

@dlt.table(
    name="claim_images_meta",
    comment="Raw accident image claim metadata ingested from object storage",
    table_properties={"quality": "bronze"}
)

def raw_images():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/smart_claims_drv/00_landing/claims/metadata/")
    )