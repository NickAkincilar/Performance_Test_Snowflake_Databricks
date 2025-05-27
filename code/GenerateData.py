from pyspark.sql.functions import col, explode, sequence, lit, expr, rand, monotonically_increasing_id
from datetime import datetime, timedelta
import pandas as pd
import random
import numpy as np

# Inputs
start_date = "2022-01-01"
end_date = "2024-12-31"
num_rows = 240000
#num_rows = 24000000000
sales_growth_pct = 1.5  # 150% increase over the entire period

# Define 16 product categories
categories = [
    "Beverages", "Bakery", "Dairy", "Meat", "Seafood", "Produce",
    "Frozen Foods", "Snacks", "Canned Goods", "Dry Goods", "Condiments",
    "Personal Care", "Household", "Pet Supplies", "Baby Products", "Health"
]

# Sample product names per category
sample_products_by_category = {
    "Beverages": ["Coca-Cola", "Pepsi", "Gatorade", "Red Bull", "Tropicana Orange Juice"],
    "Bakery": ["Whole Wheat Bread", "Croissant", "Bagel", "Blueberry Muffin", "Sourdough Bread"],
    "Dairy": ["2% Milk", "Greek Yogurt", "Cheddar Cheese", "Butter", "Cream Cheese"],
    "Meat": ["Chicken Breast", "Ground Beef", "Pork Chops", "Bacon", "Turkey Sausage"],
    "Seafood": ["Salmon Fillet", "Shrimp", "Tuna Steak", "Cod Fillet", "Crab Legs"],
    "Produce": ["Bananas", "Apples", "Baby Carrots", "Spinach", "Grapes"],
    "Frozen Foods": ["Frozen Pizza", "Ice Cream", "Frozen Vegetables", "Frozen Burritos", "Frozen Lasagna"],
    "Snacks": ["Potato Chips", "Granola Bars", "Popcorn", "Pretzels", "Chocolate Bars"],
    "Canned Goods": ["Canned Corn", "Canned Tuna", "Canned Beans", "Canned Tomatoes", "Soup"],
    "Dry Goods": ["Spaghetti", "Rice", "Lentils", "Flour", "Sugar"],
    "Condiments": ["Ketchup", "Mayonnaise", "Soy Sauce", "Hot Sauce", "BBQ Sauce"],
    "Personal Care": ["Shampoo", "Toothpaste", "Deodorant", "Body Wash", "Lotion"],
    "Household": ["Laundry Detergent", "Paper Towels", "Dish Soap", "Trash Bags", "Toilet Paper"],
    "Pet Supplies": ["Dog Food", "Cat Litter", "Bird Seed", "Pet Shampoo", "Dog Treats"],
    "Baby Products": ["Diapers", "Baby Wipes", "Baby Shampoo", "Formula", "Pacifiers"],
    "Health": ["Vitamin C", "Pain Reliever", "Allergy Medicine", "Thermometer", "First Aid Kit"]
}

# Generate 1000 unique products
product_list = []
for i in range(1000):
    category = random.choice(categories)
    product_name = random.choice(sample_products_by_category[category])
    full_name = f"{product_name} ({category})"
    unit_price = round(random.uniform(1.0, 100.0), 2)
    product_list.append((i + 1, full_name, category, unit_price))

products_df = spark.createDataFrame(product_list, ["product_id", "product_name", "category", "unit_price"])
product_bc = spark.sparkContext.broadcast(product_list)

# Generate 67 unique stores
stores = [(i + 1, f"Store #{i + 1:03d}") for i in range(67)]
stores_df = spark.createDataFrame(stores, ["store_id", "store_name"])
stores_bc = spark.sparkContext.broadcast(stores)

# Prepare date info
start = datetime.strptime(start_date, "%Y-%m-%d")
end = datetime.strptime(end_date, "%Y-%m-%d")
date_range = pd.date_range(start=start, end=end).to_pydatetime().tolist()
date_range_bc = spark.sparkContext.broadcast(date_range)

def generate_rows(partition_index, iterator):
    rng = random.Random(partition_index)
    rows = []
    for _ in iterator:
        while True:
            sale_date = rng.choice(date_range_bc.value)
            month = sale_date.month
            day_progress = (sale_date - start).days / ((end - start).days + 1)
            base_order_prob = 1 + sales_growth_pct * day_progress

            if month in [4, 12]:
                base_order_prob *= 1.3
            elif month in [5, 1]:
                base_order_prob *= 0.9

            if rng.random() <= base_order_prob / (1 + sales_growth_pct):
                product = rng.choice(product_bc.value)
                store = rng.choice(stores_bc.value)
                quantity = int(np.clip(np.random.normal(loc=5, scale=3), 1, 20))
                sales_total = round(product[3] * quantity, 2)
                rows.append((sale_date, store[0], product[0], quantity, sales_total))
                break
    return iter(rows)

# Parallel row generation
generated_rdd = spark.sparkContext.range(num_rows).mapPartitionsWithIndex(generate_rows)
sales_df = generated_rdd.toDF(["order_date", "store_id", "product_id", "quantity", "sales_total"])

# Save as delta table
sales_df.write.format("delta").mode("overwrite").saveAsTable("contoso_sales")
products_df.write.format("delta").mode("overwrite").saveAsTable("contoso_products")
stores_df.write.format("delta").mode("overwrite").saveAsTable("benchmark_tests.contoso_stores")
