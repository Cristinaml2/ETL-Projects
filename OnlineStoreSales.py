
# Extraction:
#Download the CSV dataset and load it into your Python environment using pandas
import pandas as pd

# Assuming you have downloaded the CSV file and it is in the same directory as your Python script
csv_file_path = "Online store sales .csv"  # Replace "your_file_name.csv" with the actual file name

# Load the CSV data into a pandas DataFrame
df = pd.read_csv(csv_file_path)

# Print the DataFrame to see its contents
#print(df)

#-----------------------------------------------------------------
# Transformation:

# 1. Handling Missing Values
mean_age = df["Customer Name"]
df["Customer Name"].fillna('No name', inplace=True)

# 2. Handling Duplicate Values
# Drop duplicate rows
df.drop_duplicates(inplace=True)

# Print the cleaned DataFrame
#print(df)


# Convert "Order Date" column to a proper date format
# pd.to_datetime() to convert the "Order Date" column from a string format to a proper datetime format. 
# Now, the "Order Date" column is represented as 
# datetime objects in the DataFrame, which allows for easier date-related operations and analyses.
df["Order Date"] = pd.to_datetime(df["Order Date"])

# Print the DataFrame after converting the date format
#print(df)

# Calculate the total sales amount (Quantity Sold * Price Per Unit) for each order
df["Total Sales Amount"] = df["Quantity Sold"] * df["Price Per Unit"]

# Print the DataFrame with the total sales amount
#print(df)

# Summarize the sales data by product
sales_by_product = df.groupby(["Product ID", "Product Name"]).agg({
    "Quantity Sold": "sum",
    "Price Per Unit": "mean",
    "Order Date": "count"
}).reset_index()

# Summarize the sales data by customer
sales_by_customer = df.groupby("Customer Name").agg({
    "Quantity Sold": "sum",
    "Price Per Unit": "mean",
    "Order Date": "count"
}).reset_index()

# Summarize the sales data by order date
sales_by_order_date = df.groupby("Order Date").agg({
    "Quantity Sold": "sum",
    "Price Per Unit": "mean",
    "Order ID": "count"
}).reset_index()

# Print the summarized dataframes
#print("Sales by Product:")
#print(sales_by_product)

#print("\nSales by Customer:")
#print(sales_by_customer)

#print("\nSales by Order Date:")
#print(sales_by_order_date)

#-----------------------------------------------------------------
# Loading:
# Create a new SQLite database and connect to it using Python (you can use the `sqlite3` library

import sqlite3

# Connect to or create an SQLite database (if it doesn't exist)
conn = sqlite3.connect("sales_data.db")

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Define a SQL command to create a table
create_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    OrderID INTEGER PRIMARY KEY,
    CustomerName TEXT,
    ProductID TEXT,
    ProductName TEXT,
    QuantitySold INTEGER,
    PricePerUnit REAL,
    OrderDate DATE
);
"""

# Execute the create table command
cursor.execute(create_table_query)

# Commit changes and close the connection
conn.commit()
conn.close()

#print("Database and table created successfully.")


# - Design an appropriate schema for the data and create tables to store the cleaned and transformed data.
# - Load the cleaned data into the respective tables in the database.

# Sample cleaned DataFrame
cleaned_df = pd.DataFrame(df)

print(cleaned_df)

# Connect to or create an SQLite database (if it doesn't exist)
conn = sqlite3.connect("sales_data.db")

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Define a SQL command to create the "orders" table
create_orders_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    OrderID INTEGER PRIMARY KEY,
    CustomerName TEXT,
    ProductID TEXT,
    QuantitySold INTEGER,
    PricePerUnit REAL,
    OrderDate DATE
);
"""

# Execute the create table command for "orders" table
cursor.execute(create_orders_table_query)

# Define a SQL command to create the "products" table
create_products_table_query = """
CREATE TABLE IF NOT EXISTS products (
    ProductID TEXT PRIMARY KEY,
    ProductName TEXT
);
"""

# Execute the create table command for "products" table
cursor.execute(create_products_table_query)

# Load the cleaned data into the "orders" table
cleaned_df.to_sql("orders", conn, if_exists="replace", index=False)

# Insert product data into the "products" table
product_data = [
    ("P1001", "Widget A"),
    ("P1002", "Widget B"),
    ("P1003", "Widget C"),
    ("P1004", "Widget D"),
    ("P1005", "Widget E"),
    ("P1006", "Widget F"),
    ("P1007", "Widget G"),
]

insert_products_query = "INSERT OR REPLACE INTO products (ProductID, ProductName) VALUES (?, ?)"

cursor.executemany(insert_products_query, product_data)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data loaded into the database tables successfully.")

print(product_data)
