from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pandas as pd
from faker import Faker
import random
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize Spark context and session
conf = SparkConf().setAppName("SyntheticDataGeneration") # we set the app name
sc = SparkContext.getOrCreate(conf=conf) # entry point for spark
spark = SparkSession.builder \
    .config("spark.pyspark.python", "C:\\Users\\Kays\\miniconda3\\python.exe")\
    .config("spark.pyspark.driver.python", "C:\\Users\\Kays\\miniconda3\\python.exe")\
    .getOrCreate()

# Start Faker
fake = Faker()


# Generate synthetic data
def generate_synthetic_data(num_records):
    data = []
    for _ in range(num_records):
        data.append({
            'transaction_id': fake.uuid4(),
            'transaction_date': fake.date_time_this_decade(),
            'customer_id': fake.random_int(min=1000, max=10000),
            'product_id': fake.random_int(min=1, max=100),
            'quantity': fake.random_int(min=1, max=10),
            'price': round(fake.random_int(min=10, max=100), 2),
            'total_price': round(fake.random_int(min=10, max=100), 2),
            'payment_method': random.choice(['credit_card', 'debit_card', 'cash', 'Paypal', 'Bank_transfer']),
            'transaction_type': random.choice(['purchase', 'return']),
            'country': fake.country()
        })
    return data

# Generate 1000 records of payment data
payment_data = generate_synthetic_data(1000)

# Convert to Pandas DataFrame before converting to spark dataframe,
#  repartition the dataframe so that spark can handle it
payment_df = pd.DataFrame(payment_data)
# Convert to spark dataframe
spark_payment_df = spark.createDataFrame(payment_df)

# the dataframe is divided into 2 partitions, spark is a distributed system
spark_payment_df = spark_payment_df.repartition(2) 
spark_payment_df.show(10)

# Display the schema
spark_payment_df.printSchema() # print the schema of the dataframe

# Display the summary
spark_payment_df.describe().show()

# Display the columns
spark_payment_df.columns
