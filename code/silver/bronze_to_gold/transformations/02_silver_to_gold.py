import geopy
import pandas as pd
from typing import Iterator
import random
from pyspark.sql.functions import (
    col, to_date, date_format, trim, initcap, split, size, when, concat, lit, abs, to_timestamp, regexp_extract, round, avg, pandas_udf
)

catalog = "smart_claims_drv"
silver_schema = "02_silver"
gold_schema = "03_gold"

# --- Functions ---

def geocode(geolocator, address):
    try:
        #Skip the API call for faster demo (remove this line for ream)
        return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
        location = geolocator.geocode(address)
        if location:
            return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      
@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))

# --- TELEMATICS ---

@dlt.table(
    name= f"{catalog}.{gold_schema}.aggregated_telematics",
    comment= "Average Telematics",
    table_properties = {
        "quality" : "gold"
    }    
)

def telematics():
    df = dlt.read(f"{catalog}.{silver_schema}.telematics")
    return (
        df \
            .groupBy("chassis_no") \
            .agg(
                avg("speed").alias("telematics_speed"),
                avg("latitude").alias("telematics_latitude"),
                avg("longitude").alias("telematics_longitude")
            )
    )

    # --- CLAIM-POLICY ---

@dlt.table(
    name= f"{catalog}.{gold_schema}.customer_claim_policy",
    comment= "Curated claim joined with policy records",
    table_properties = {
        "quality" : "gold"
    }    
)

def customer_claim_policy():

    df_policy = dlt.read(f"{catalog}.{silver_schema}.policy")

    df_claim = dlt.read(f"{catalog}.{silver_schema}.claim")

    df_customer = dlt.read(f"{catalog}.{silver_schema}.customer")

    df_claim_policy = df_claim.join(df_policy, "policy_no")

    return (
        df_claim_policy.join(df_customer, df_claim_policy.CUST_ID == df_customer.customer_id)
    )

    # --- CLAIM - POLICY - TELEMATICS ---

@dlt.table(
    name= f"{catalog}.{gold_schema}.customer_claim_policy_telematics",
    comment= "Curated claim joined with policy records",
    table_properties = {
        "quality" : "gold"
    }    
)

def customer_claim_policy_telematics():
    df_telematics = dlt.read(f"{catalog}.{gold_schema}.aggregated_telematics")

    df_customer_claim_policy = dlt.read(f"{catalog}.{gold_schema}.customer_claim_policy").where("BOROUGH is not null")

    return (
        df_customer_claim_policy \
            .withColumn("lat_long", get_lat_long(col("address"))) \
            .join(df_telematics, on="chassis_no")
    )
