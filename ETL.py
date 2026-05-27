from pyspark .sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("e-commerce").getOrCreate()

def create_customers_df():
    
    customers_schema = StructType([
        StructField('customer_id', StringType(), True),
        StructField('signup_date', StringType(), True),
        StructField('country', StringType(), True),
        StructField('customer_tier', StringType(), True),
        StructField('email', StringType(), True),
    ])
    
    customers_df = spark.read.csv('./e-commerce_pipeline/csv/customers.csv', header=True, schema=customers_schema)
    
    order_items_schema = StructType([
        StructField('item_id', StringType(), True),
        StructField('order_id', StringType(), True),
        StructField('product_id', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('unit_price', FloatType(), True),
        StructField('category', StringType(), True)
    ])
    
    order_items_df = spark.read.csv('./e-commerce_pipeline/csv/order_items.csv', header=True, schema=order_items_schema)
    
    orders_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('order_date', StringType(), True),
        StructField('status', StringType(), True),
        StructField('total_amount', FloatType(), True),
        StructField('discount_pct', FloatType(), True)
    ])
    
    orders_df = spark.read.csv('./e-commerce_pipeline/csv/orders.csv', header=True, schema=orders_schema)
    
    returns_schema = StructType([
        StructField('return_id', StringType(), True),
        StructField('order_id', StringType(), True),
        StructField('return_date', StringType(), True),
        StructField('reason', StringType(), True),
        StructField('refund_amount', FloatType(), True)
    ])
    
    returns_df = spark.read.csv('./e-commerce_pipeline/csv/returns.csv', header=True, schema=returns_schema)
    
    
    return customers_df, order_items_df, orders_df, returns_df

    
def transform_data(customers_df, orders_df, returns_df, order_items_df):
    
    #DROPPING DUPLICATES
    customers_df = customers_df.dropDuplicates()
    orders_df = orders_df.dropDuplicates()
    returns_df = returns_df.dropDuplicates()
    order_items_df = order_items_df.dropDuplicates()
    
   #DROP ROWS WHERE ORDER_ID OR CUSTOMER_ID IS NULL
    customers_df = customers_df.na.drop(subset=["customer_id"])
    orders_df= orders_df.na.drop(subset=["customer_id", "order_id"])
    returns_df = returns_df.na.drop(subset=["order_id"])
    order_items_df = order_items_df.na.drop(subset=["order_id"]) 
    
    #CUSTOMER_TIER TO LOWER_CASE
    customers_df = customers_df.withColumn(
        'customer_tier',
        lower(customers_df['customer_tier'])
    )
    
    #DATE NORMALISATION
    #date normalisation expr() that allows writing SQL-like expressions inside spark
    customers_df = customers_df.withColumn(
        'signup_date',#creates or replaces the column signup_date 
        coalesce(
            expr("try_to_date(signup_date, 'yyyy-MM-dd')"), expr("try_to_date(signup_date, 'dd/MM/yyyy')")
            )
        ) 
    
    orders_df = orders_df.withColumn(
        'order_date',
        coalesce(
            expr("try_to_date(order_date, 'yyyy-MM-dd')"), expr("try_to_date(order_date, 'dd/MM/yyyy')")
        )
    )
    
    returns_df = returns_df.withColumn(
        'return_date',
        coalesce(
            expr("try_to_date(return_date, 'yyyy-MM-dd')"), expr("try_to_date(return_date, 'dd/MM/yyyy')")
        )
    )
    
    #BOOLEAN COLUMN FOR -VE VALUES ON TOTAL AMOUNT COLUMN
    orders_df = orders_df.withColumn(
        'is_negative_amount', #automatically auto assigns true/false base on the defined condition
        col("total_amount") < 0
    )
    
    return customers_df, orders_df, returns_df, order_items_df

customers_df, order_items_df, orders_df, returns_df = create_customers_df()
customers_df.show(truncate=False)
order_items_df.show()
orders_df.show()
returns_df.show(truncate=False)


transformed_customers_df, transformed_orders_df, transformed_returns_df, transformed_order_items_df = transform_data(customers_df, orders_df, returns_df, order_items_df)
transformed_customers_df.show(truncate=False)
transformed_order_items_df.show()
transformed_orders_df.show()
transformed_returns_df.show(truncate=False)
