from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap, split, size, when, concat, lit, abs, to_timestamp, regexp_extract, round
)

catalog = "smart_claims_drv"
bronze_schema = "01_bronze"
silver_schema = "02_silver"

# --- CLEAN CLAIM ---

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
            .drop("_rescued_data") #Não tem
            
    )

    # --- CLEAN POLICY ---

@dlt.table(
    name =f"{catalog}.{silver_schema}.policy",
    comment= "Cleaned Policy",
    table_properties= {
        "quality": "silver"
    }
)
@dlt.expect("valid_policy_no", "POLICY_NO IS NOT NULL")
def policy():
    return (
        dlt.readStream(f"{catalog}.{bronze_schema}.policies") \
            .withColumn("premium", abs("premium")) \
            .withColumn("POL_ISSUE_DATE", to_date(col("POL_ISSUE_DATE"), "yyyy-MM-dd"))
            .withColumn("POL_EFF_DATE", to_date(col("POL_EFF_DATE"), "yyyy-MM-dd"))
            .withColumn("POL_EXPIRY_DATE", to_date(col("POL_EXPIRY_DATE"), "yyyy-MM-dd"))
            .withColumn("MODEL_YEAR", col("MODEL_YEAR").cast("bigint"))
    )


# --- CLEAN TELEMATICS ---

@dlt.table(
    name = f"{catalog}.{silver_schema}.telematics",
    comment = "Cleaned Telematics",
    table_properties = {
        "quality" : "silver"
    }
)

@dlt.expect("valid_coordinates", "latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180")

def telematics():
    return(
        dlt.readStream(f"{catalog}.{bronze_schema}.telematics") \
            .withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("speed", round(col("speed"), 2))
    )
    
# --- CLEAN CUSTOMER ---

@dlt.table(
    name = f"{catalog}.{silver_schema}.customer",
    comment = "Cleaned Customer",
    table_properties = {
        "quality" : "silver"
    }
)

@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")

def customer():
    return(
        dlt.readStream(f"{catalog}.{bronze_schema}.customers") \
            .withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy")) \
            .withColumn("first_name", split(col("name"), ", ")[1]) \
            .withColumn("last_name", split(col("name"), ", ")[0]) \
            .withColumn("address", concat(col("borough"), lit(", "), col("zip_code")))
            .drop("name")        
    )

# --- CLEAN TRAINING IMAGES ---

@dlt.table(
    name = f"{catalog}.{silver_schema}.training_images",
    comment = "Cleaned training_images",
    table_properties = {
        "quality" : "silver"
    }
)

def training_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.training_images")
    return(
        df \
            .withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2))
    )

# --- CLEAN CLAIM IMAGES ---

@dlt.table(
    name = f"{catalog}.{silver_schema}.claim_images",
    comment = "Cleaned claim_images",
    table_properties = {
        "quality" : "silver"
    }
)

def claim_images():
    df = dlt.readStream(f"{catalog}.{bronze_schema}.claim_images")
    return(
        df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
    )
