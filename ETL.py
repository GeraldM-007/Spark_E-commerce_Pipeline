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

#convert timestamp to seconds
orders_timestamp_in_seconds_df = transformed_orders_df.withColumn(
    'order_timestamp',
    col('order_date').cast('timestamp').cast('long')
    )

rollingCountWindow = Window.partitionBy('customer_id').orderBy('order_timestamp').rangeBetween(-6 *86400, 0)

orders_rolling_count_df = orders_timestamp_in_seconds_df.withColumn(
    '7d_rolling_order_count',
    count('*').over(rollingCountWindow)
    )

#REVENUE

orders_joined_items_df = derived_orders_df.join(transformed_order_items_df, on='order_id', how='inner')
orders_joined_items_df =orders_joined_items_df.withColumn('month', date_format("order_date", 'yyyy-MM'))

#calculate the total revenue for each category per month
rev_per_category_month_df = orders_joined_items_df.groupBy('month', 'category').agg(sum('net_amount').alias('category_revenue_per_month')).orderBy('month')

#calculate the total revenue per month
rev_per_month_df = orders_joined_items_df.groupBy('month').agg(sum('net_amount').alias('monthly_total_revenue')).orderBy('month')

#join the total rev per category df to the monthly total rev df
category_month_df = rev_per_category_month_df.join(rev_per_month_df, on='month', how='inner').orderBy('month')

#calculate the revenue share
revenue_share_df = category_month_df.withColumn('revenue_share', col('category_revenue_per_month') / col('monthly_total_revenue')).orderBy('month')

print('Start Window Functions')

ranked_customer_lifetime_net_spend_df.show()

orders_rolling_count_df.show()

revenue_share_df.show()

print('End window functions\n')

#RETURN ANALYSIS

#Return rate per category
returnRateWindow = Window.partitionBy('category')

order_count_df = orders_joined_items_df.withColumn(
    'order_count_per_category',
    count('order_id').over(returnRateWindow)
)

returns_joined_items_df = transformed_returns_df.join(transformed_order_items_df, on='order_id', how='inner')

return_count_df = returns_joined_items_df.withColumn(
    'return_count_per_category',
    count('return_id').over(returnRateWindow)
)

return_rate_per_category_df = order_count_df.join(return_count_df, on='category', how='inner')

return_rate_per_category_df = return_rate_per_category_df.withColumn('return_rate', col('order_count_per_category') / col('return_count_per_category'))

return_rate_per_category_df = return_rate_per_category_df.select(col('category'), col('order_count_per_category'), col('return_count_per_category'), col('return_rate'))

#Return rate per customer tier
return_cust_df = transformed_returns_df.join(cust_orders_df, on='order_id', how='inner')

CustomerTierWindow = Window.partitionBy('customer_tier')

order_count_per_cust_tier_df = cust_orders_df.withColumn(
    'order_count_per_tier',
    count('order_id').over(CustomerTierWindow)
)

return_count_per_cust_tier_df = return_cust_df.withColumn(
    'return_count_per_tier',
    count('return_id').over(CustomerTierWindow)
)

return_rate_per_tier_df = return_count_per_cust_tier_df.join(order_count_per_cust_tier_df, on='customer_id', how='inner')

return_rate_per_tier_df = return_rate_per_tier_df.withColumn('return_rate_per_tier', col('return_count_per_tier') / col('order_count_per_tier'))

return_rate_per_tier_df = return_rate_per_tier_df.select(col('order_count_per_tier'), col('return_count_per_tier'), col('return_rate_per_tier'))

#Top 10 customers by total refund amount

#using semi join to check the total number of unique return orders thus proving existence of repeat returns
count_unique_order_returns_df = transformed_returns_df.join(cust_orders_df, on='order_id', how='semi')

order_returns_df = transformed_returns_df.join(cust_orders_df, on='order_id', how='inner')

#calculate the total refund amount per customer
total_refund_amount_df = transformed_returns_df.groupBy('return_id').agg(sum('refund_amount').alias('total_refund_amount'))

#Join the customers df to the created total refund amount df and substitute nulls in the total_refund_amount colmn with zero
customer_total_refund_amount_df = return_cust_df.join(total_refund_amount_df, on='return_id', how='left').fillna({'total_refund_amount': 0})

#define a window partition to order customers by thier total refund amount
top_10_df = customer_total_refund_amount_df.orderBy(col('total_refund_amount').desc())

#BOOLEAN COLUMN FOR where refund exceeds order
#selecting the order_id and net_amount column from the derived_orders_df
orderId_netamount_df = derived_orders_df.select(col('order_id'), col('net_amount'))

refund_joined_orders_df = customer_total_refund_amount_df.join(orderId_netamount_df, on='order_id', how='inner')

withBoolean_return_analysis_df = refund_joined_orders_df.withColumn(
    'total_refund_amount > net_amount',
    col('total_refund_amount') > col('net_amount')
)

refund_vs_net_df = withBoolean_return_analysis_df.select( col('return_id'), col('total_refund_amount'), col('net_amount'), col('total_refund_amount > net_amount'))

print('Start Return Analysis')

#print return rate per category
return_rate_per_category_df.show()

#Print return rate per customer tier
return_rate_per_tier_df.show()

#print the total refund amount
customer_total_refund_amount_df.show()
print(customer_total_refund_amount_df.count())
top_10_df.show(10)

refund_joined_orders_df.show()
withBoolean_return_analysis_df.show()
refund_vs_net_df.show()

print("Return Analysis End")

#OUTPUT AND PARTITIONING

customer_total_refund_amount_df.write.mode("overwrite").parquet("./parquet/cust_total_refund_amount")

refund_joined_orders_df.write.mode('overwrite').partitionBy("year", "month").parquet("./parquet/refund_joined_orders")

category_month_df.write.mode("overwrite").partitionBy("year", "month").parquet("./parquet/revenue_dist_per_month")

spark.stop()