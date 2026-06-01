from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


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
print('Completed printing the Original DFs\n')


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

print('Rejected DFs Start')
rejected_orders_df.show()
print("Rejected DFs End\n")

    
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
withBoolean_orders_df = transformed_orders_df.withColumn(
        'is_negative_amount', #automatically auto assigns true/false based on the defined condition
        col("total_amount") < 0
    )


transformed_customers_df.show(truncate=False)
transformed_order_items_df.show()
transformed_orders_df.show()
transformed_returns_df.show(truncate=False)
withBoolean_orders_df.show()
print('Completed printing the transformed data frames\n')

#JOINS

cust_orders_df = transformed_orders_df.join(transformed_customers_df, on='customer_id', how='inner') 

cust_orders_items_df = cust_orders_df.join(transformed_order_items_df, on='order_id', how='inner')



print('JOINS start')
cust_orders_df.show()
cust_orders_items_df.show()
print('JOINS End\n')

#ORPHANED ITEMS
orphaned_orders_df = transformed_order_items_df.join(transformed_orders_df, on='order_id', how='anti')

print('Start Orphaned DFs')
orphaned_orders_df.show()
print(orphaned_orders_df.count())
print('End Orphaned DFs\n')


derived_orders_df = transformed_orders_df.withColumn(
    'net_amount',
    col('total_amount') * (1-col('discount_pct')/100)
)

print('Derived DFs Start')
derived_orders_df.show()
print(derived_orders_df.count())
print('Derived DFs End\n')

#AGGREGATIONS

#using semi join to check the total number of unique customer orders thus proving existence of repeat orders
count_unique_customer_orders_df = transformed_customers_df.join(transformed_orders_df, on='customer_id', how='semi')

#calculate the lifetime net spend for each customer
lifetime_net_spend_df = derived_orders_df.groupBy('customer_id').agg(sum('net_amount').alias('lifetime_net_spend'))

#Join the customers df to the created lifetime net spend df and substitute nulls in the lifetime spend colmn with zero
customer_lifetime_net_spend_df = transformed_customers_df.join(lifetime_net_spend_df, on='customer_id', how='left').fillna({'lifetime_net_spend': 0})

#define a window partition to partition the customers by country and order by their lifetime spending
windowPartition = Window.partitionBy('country').orderBy(col('lifetime_net_spend').desc())

#create a df ranking the customers by spend per country 
ranked_customer_lifetime_net_spend_df = customer_lifetime_net_spend_df.withColumn('customer_spend_rank_by_country', rank().over(windowPartition))

#7 Day Rolling Order Count


print('Start Window Functions')
count_unique_customer_orders_df.show()
print(count_unique_customer_orders_df.count())

lifetime_net_spend_df.show()
print(lifetime_net_spend_df.count())

customer_lifetime_net_spend_df.show()
print(customer_lifetime_net_spend_df.count())

ranked_customer_lifetime_net_spend_df.show()
print('End window functions')

