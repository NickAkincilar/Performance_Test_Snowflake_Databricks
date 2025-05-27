# Performance Test Comparison Snowflake vs. Databricks
Running Performance test between Snowflake &amp; Databricks using 24 Billion row dataset

### 1. Create an internal stage in Snowflake 
Upload the 3 sample data files into the stage using Snowsight.

[Sample Files](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks/tree/main/SampleFiles)

### 2. Setup the environment by creating tables & ingesting the data.

Test includes 3 files. Sales, Stores & Products. Use the following SQL Script to build the environment. 
[Sample Files](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks/tree/main/SampleFiles)

### 3. Upscale the 240K row contoso_sales_240K table to 24 Billion by replicating row 100K times

Execute the appropriate queries on each to upscale the 240K sample to 24B rows.

``` sql
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


 -- Upscale to 24B rows for Databricks
CREATE OR REPLACE TABLE contoso_sales_24B AS
SELECT 
  original.* 
FROM contoso_sample_sales AS original
JOIN (
  SELECT explode(sequence(1, 100000)) AS replicate_id
) replicator
ON true;

```

### 4. Run all 16 Queries 2 or 3 times to get the lowest number.
Use the following [Queries](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks/blob/main/code/GenerateData.py) to execute on each platform

### 5.** Optional *** Generate your own data instead of using the 240K row sample dataset.
Use the following Python code in Databricks to generate a sample dataset that looks realistic.

[Databricks Sample Sales Data Generator](https://github.com/NickAkincilar/Performance_Test_Snowflake_Databricks/blob/main/code/GenerateData.py)



