-- Créer l'extension dblink si elle n'existe pas
CREATE EXTENSION IF NOT EXISTS dblink;

-- Insertion des données dans la table dimratecodes
INSERT INTO dimratecodes (ratecodeid, ratedescription)
SELECT DISTINCT "ratecodeid", 
    CASE 
        WHEN "ratecodeid" = 1 THEN 'Standard rate'
        WHEN "ratecodeid" = 2 THEN 'JFK'
        WHEN "ratecodeid" = 3 THEN 'Newark'
        WHEN "ratecodeid" = 4 THEN 'Nassau or Westchester'
        WHEN "ratecodeid" = 5 THEN 'Negotiated fare'
        WHEN "ratecodeid" = 6 THEN 'Group ride'
        ELSE 'Unknown'
    END 
FROM dblink(
    'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin', 
    'SELECT DISTINCT "ratecodeid" FROM "nyc_raw" WHERE "ratecodeid" IS NOT NULL'
) AS source_data("ratecodeid" INTEGER);

-- Insertion des données dans la table dimpaymenttypes
INSERT INTO dimpaymenttypes (paymenttypeid, paymentdescription)
SELECT DISTINCT "payment_type", 
    CASE 
        WHEN "payment_type" = 1 THEN 'Credit card'
        WHEN "payment_type" = 2 THEN 'Cash'
        WHEN "payment_type" = 3 THEN 'No charge'
        WHEN "payment_type" = 4 THEN 'Dispute'
        WHEN "payment_type" = 5 THEN 'Unknown'
        WHEN "payment_type" = 6 THEN 'Voided trip'
        ELSE 'Other'
    END 
FROM dblink(
    'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin',  
    'SELECT DISTINCT "payment_type" FROM "nyc_raw" WHERE "payment_type" IS NOT NULL'
) AS source_data("payment_type" INTEGER);

-- Insertion des données dans la table dimlocations (pulocationid)
INSERT INTO dimlocations (locationid, zonename, borough)
SELECT DISTINCT "pulocationid", 
    'Unknown' AS zonename,  -- Valeur par défaut pour zonename
    'Unknown' AS borough    -- Valeur par défaut pour borough
FROM dblink(
    'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin',  
    'SELECT DISTINCT "pulocationid" FROM "nyc_raw" WHERE "pulocationid" IS NOT NULL'
) AS source_data("pulocationid" INTEGER)
WHERE NOT EXISTS (
    SELECT 1 FROM dimlocations WHERE locationid = source_data."pulocationid"
);

-- Insertion des données dans la table facttrips
INSERT INTO facttrips (
    vendorid, pulocationid, dulocationid, ratecodeid, payment_type, fare_amount, 
    extra, mta_tax, improvement_surcharge, tip_amount, tolls_amount, total_amount, 
    congestion_surcharge, airport_fee, trip_distance, passenger_count, 
    tpep_pickup_datetime, tpep_dropoff_datetime
)
SELECT 
    "vendorid", 
    "pulocationid", 
    "dulocationid", 
    "ratecodeid", 
    "payment_type", 
    "fare_amount", 
    "extra", 
    "mta_tax", 
    "improvement_surcharge", 
    "tip_amount", 
    "tolls_amount", 
    "total_amount", 
    "congestion_surcharge", 
    "airport_fee", 
    "trip_distance", 
    "passenger_count", 
    "tpep_pickup_datetime", 
    "tpep_dropoff_datetime"
FROM dblink(
    'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin',  
    'SELECT "vendorid", "pulocationid", "dulocationid", "ratecodeid", "payment_type", 
            "fare_amount", "extra", "mta_tax", "improvement_surcharge", "tip_amount", 
            "tolls_amount", "total_amount", "congestion_surcharge", "airport_fee", 
            "trip_distance", "passenger_count", "tpep_pickup_datetime", "tpep_dropoff_datetime"
    FROM "nyc_raw" 
    WHERE "vendorid" IS NOT NULL AND "pulocationid" IS NOT NULL AND "dulocationid" IS NOT NULL'
) AS source_data(
    "vendorid" INTEGER, "pulocationid" INTEGER, "dulocationid" INTEGER, "ratecodeid" INTEGER, 
    "payment_type" INTEGER, "fare_amount" FLOAT, "extra" FLOAT, "mta_tax" FLOAT, 
    "improvement_surcharge" FLOAT, "tip_amount" FLOAT, "tolls_amount" FLOAT, 
    "total_amount" FLOAT, "congestion_surcharge" FLOAT, "airport_fee" FLOAT, 
    "trip_distance" FLOAT, "passenger_count" INTEGER, 
    "tpep_pickup_datetime" TIMESTAMP, "tpep_dropoff_datetime" TIMESTAMP
);

-- Insertion des données dans la table dimvendors
INSERT INTO dimvendors (vendorid, vendorname)c
SELECT DISTINCT "vendorid", 
    CASE 
        WHEN "vendorid" = 1 THEN 'Creative Mobile Technologies, LLC' 
        WHEN "vendorid" = 2 THEN 'VeriFone Inc.' 
        ELSE 'Unknown' 
    END 
FROM dblink(
    'host=data-warehouse port=5432 dbname=nyc_warehouse user=postgres password=admin',  
    'SELECT DISTINCT "vendorid" FROM "nyc_raw"'
) AS source_data("VendorID" INTEGER);
