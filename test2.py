from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("e-commerce").getOrCreate()

#DATA INGESTION AND SCHEMA ENFORCEMENT

customers_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('signup_date', StringType(), True),
    StructField('country', StringType(), True),
    StructField('customer_tier', StringType(), True),
    StructField('email', StringType(), True),
    ])
    
original_customers_df = spark.read.csv('./e-commerce_pipeline/csv/customers.csv', header=True, schema=customers_schema)
    
order_items_schema = StructType([
        StructField('item_id', StringType(), True),
        StructField('order_id', StringType(), True),
        StructField('product_id', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('unit_price', FloatType(), True),
        StructField('category', StringType(), True)
    ])
    
original_order_items_df = spark.read.csv('./e-commerce_pipeline/csv/order_items.csv', header=True, schema=order_items_schema)
    
orders_schema = StructType([
        StructField('order_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('order_date', StringType(), True),
        StructField('status', StringType(), True),
        StructField('total_amount', FloatType(), True),
        StructField('discount_pct', FloatType(), True)
    ])
    
original_orders_df = spark.read.csv('./e-commerce_pipeline/csv/orders.csv', header=True, schema=orders_schema)
    
returns_schema = StructType([
        StructField('return_id', StringType(), True),
        StructField('order_id', StringType(), True),
        StructField('return_date', StringType(), True),
        StructField('reason', StringType(), True),
        StructField('refund_amount', FloatType(), True)
    ])
    
original_returns_df = spark.read.csv('./e-commerce_pipeline/csv/returns.csv', header=True, schema=returns_schema)

#printing the orginal dataframes
original_customers_df.show(truncate=False)
original_order_items_df.show()
original_orders_df.show()
original_returns_df.show(truncate=False)
print('Completed printing the Original\n')


#DATA QUALITY AND CLEANING

    #DROPPING DUPLICATES
customers_df = original_customers_df.dropDuplicates()
orders_df = original_orders_df.dropDuplicates()
returns_df = original_returns_df.dropDuplicates()
order_items_df = original_order_items_df.dropDuplicates()

'''
#Handling the Duplicates: does not work
duplicate_customers_df = original_customers_df.subtract(customers_df)
duplicate_orders_df = original_orders_df.subtract(orders_df)
duplicate_returns_df = original_returns_df.subtract(returns_df)
duplicate_order_items_df = original_order_items_df.subtract(order_items_df)

print('Printing Duplicates')
duplicate_customers_df.show()
duplicate_orders_df.show()
duplicate_order_items_df.show()
duplicate_returns_df.show()
print('End of duplicates')
'''

#DROP ROWS WHERE ORDER_ID OR CUSTOMER_ID IS NULL
cleaned_customers_df = customers_df.na.drop(subset=["customer_id"])
cleaned_orders_df= orders_df.na.drop(subset=["customer_id", "order_id"])
cleaned_returns_df = returns_df.na.drop(subset=["order_id"])
cleaned_order_items_df = order_items_df.na.drop(subset=["order_id"]) 

#Handling NULLs gracefully
rejected_orders_df = orders_df.filter(col('customer_id').isNull() | col('order_id').isNull())

print('Rejected')
rejected_orders_df.show()
print("Rejected End\n")

    
#CUSTOMER_TIER TO LOWER_CASE
cleaned_customers_df = cleaned_customers_df.withColumn(
        'customer_tier',
        lower(customers_df['customer_tier'])
    )
    
    #DATE NORMALISATION
    #date normalisation expr() that allows writing SQL-like expressions inside spark
transformed_customers_df = cleaned_customers_df.withColumn(
        'signup_date',#creates or replaces the column signup_date 
        coalesce(
            expr("try_to_date(signup_date, 'yyyy-MM-dd')"), expr("try_to_date(signup_date, 'dd/MM/yyyy')")
            )
        ) 
    
transformed_orders_df = cleaned_orders_df.withColumn(
        'order_date',
        coalesce(
            expr("try_to_date(order_date, 'yyyy-MM-dd')"), expr("try_to_date(order_date, 'dd/MM/yyyy')")
        )
    )
    
transformed_returns_df = cleaned_returns_df.withColumn(
        'return_date',
        coalesce(
            expr("try_to_date(return_date, 'yyyy-MM-dd')"), expr("try_to_date(return_date, 'dd/MM/yyyy')")
        )
    )

transformed_order_items_df = cleaned_order_items_df
    
#BOOLEAN COLUMN FOR -VE VALUES ON TOTAL AMOUNT COLUMN
transformed_orders_df = transformed_orders_df.withColumn(
        'is_negative_amount', #automatically auto assigns true/false base on the defined condition
        col("total_amount") < 0
    )


transformed_customers_df.show(truncate=False)
transformed_order_items_df.show()
transformed_orders_df.show()
transformed_returns_df.show(truncate=False)
print('Completed printing the transformed data frames\n')

joined_df1 = transformed_orders_df.join(transformed_customers_df, on='customer_id', how='inner') 

joined_df2 = joined_df1.join(transformed_order_items_df, on='order_id', how='inner')



print('JOINS')
joined_df1.show()
joined_df2.show()
print('End JOINS\n')

#ORPHANED ITEMS
orphaned_orders_df = transformed_order_items_df.join(transformed_orders_df, on='order_id', how='anti')

print('Start Orphaned')
orphaned_orders_df.show()
print(orphaned_orders_df.count())
print('End Orphaned')


derived_orders_df = transformed_orders_df.withColumn(
    'net_amount',
    col('total_amount') * (1-col('discount_pct')/100)
)

print('Derived')
derived_orders_df.show()