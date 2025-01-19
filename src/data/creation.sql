-- Création des tables de dimension
CREATE TABLE dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(255) 
);


CREATE TABLE dim_location (
    location_id INT PRIMARY KEY, 
    borough VARCHAR(255), 
    zone VARCHAR(255) 
);

CREATE TABLE dim_ratecode (
    ratecode_id DOUBLE PRECISION PRIMARY KEY, 
    ratecode_description VARCHAR(255) 
);

CREATE TABLE dim_payment_type (
    payment_type_id BIGINT PRIMARY KEY, 
    payment_type_desc VARCHAR(255) 
);

-- Création de la table de faits
-- Création de la table de fait
CREATE TABLE FactTrips (
    TripID SERIAL PRIMARY KEY,
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    Passenger_count INT,
    Trip_distance FLOAT,
    PULocationID INT,
    DOLocationID INT,
    RateCodeID INT,
    Store_and_fwd_flag CHAR(1),
    Payment_type INT,
    Fare_amount FLOAT,
    Extra FLOAT,
    MTA_tax FLOAT,
    Improvement_surcharge FLOAT,
    Tip_amount FLOAT,
    Tolls_amount FLOAT,
    Total_amount FLOAT,
    Congestion_Surcharge FLOAT,
    Airport_fee FLOAT
);
ALTER TABLE FactTrips
ADD CONSTRAINT fk_vendor
FOREIGN KEY (VendorID) REFERENCES DimVendors(VendorID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_pulocation
FOREIGN KEY (PULocationID) REFERENCES DimLocations(LocationID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_dolocation
FOREIGN KEY (DOLocationID) REFERENCES DimLocations(LocationID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_ratecode
FOREIGN KEY (RateCodeID) REFERENCES DimRateCodes(RateCodeID);

ALTER TABLE FactTrips
ADD CONSTRAINT fk_paymenttype
FOREIGN KEY (Payment_type) REFERENCES DimPaymentTypes(PaymentTypeID);