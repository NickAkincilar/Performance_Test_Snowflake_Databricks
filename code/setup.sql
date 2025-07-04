CREATE OR REPLACE FILE FORMAT My_Parq_Format TYPE = PARQUET REPLACE_INVALID_CHARACTERS = TRUE BINARY_AS_TEXT = FALSE;


create or replace TABLE CONTOSO_STORES (
    STORE_ID NUMBER(38, 0),
    STORE_NAME VARCHAR,
    CITY VARCHAR,
    STATE VARCHAR
);


create or replace TABLE CONTOSO_PRODUCTS (
    PRODUCT_ID NUMBER(38, 0),
    PRODUCT_NAME VARCHAR,
    CATEGORY VARCHAR,
    UNIT_PRICE NUMBER(38, 2),
    FROM_DATE DATE,
    TO_DATE DATE
);


create or replace TABLE CONTOSO_SALES_240K (
    ORDER_DATE TIMESTAMP_NTZ,
    STORE_ID NUMBER(38, 0),
    PRODUCT_ID NUMBER(38, 0),
    QUANTITY NUMBER(38, 0),
    SALES_TOTAL NUMBER(38, 2)
);


COPY INTO CONTOSO_STORES
FROM
    (
        SELECT
            $1:STORE_ID::NUMBER(38, 0),
            $1:STORE_NAME::VARCHAR,
            $1:CITY::VARCHAR,
            $1:STATE::VARCHAR
        from
            '@MY_INT_STAGE'
    ) FILES = ('contoso_stores.parquet_0_0_0.snappy.parquet') FILE_FORMAT = 'My_Parq_Format' ON_ERROR = ABORT_STATEMENT;


    
COPY INTO CONTOSO_PRODUCTS
FROM
    (
        SELECT
            $1:PRODUCT_ID::NUMBER(38, 0),
            $1:PRODUCT_NAME::VARCHAR,
            $1:CATEGORY::VARCHAR,
            $1:UNIT_PRICE::NUMBER(38, 2),
            $1:FROM_DATE::DATE,
            $1:TO_DATE::DATE
        from
            '@MY_INT_STAGE'
    ) FILES = ('contoso_products.parquet_0_0_0.snappy.parquet') FILE_FORMAT = 'My_Parq_Format' ON_ERROR = ABORT_STATEMENT;

    
COPY INTO CONTOSO_SALES_240K
FROM
    (
        SELECT
            $1:ORDER_DATE::TIMESTAMP_NTZ,
            $1:STORE_ID::NUMBER(38, 0),
            $1:PRODUCT_ID::NUMBER(38, 0),
            $1:QUANTITY::NUMBER(38, 0),
            $1:SALES_TOTAL::NUMBER(38, 2)
        from
            '@MY_INT_STAGE'
    ) FILES = (
        'contoso_sales_240k.parquet_0_0_0.snappy.parquet'
    ) FILE_FORMAT = 'My_Parq_Format' ON_ERROR = ABORT_STATEMENT;



 -- Upscale to 24B rows for Snowflake
CREATE OR REPLACE TABLE contoso_sales_24B AS
with cte as
(
SELECT 
  * 
FROM contoso_sales_240K 
order by order_date, store_id, product_id
)
select * from cte AS original
JOIN (
  SELECT seq4() as replicate_id
  FROM TABLE(GENERATOR(ROWCOUNT => 100000))
) replicator
ON true;
