from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('test').getOrCreate()

raw_df = spark.read.csv('./e-commerce_pipeline/csv/order_items.csv', header=True, inferSchema = True)

raw_df.show()
print(raw_df.count())

#NULL HANDLING

#df = df.drop('Email')

#df = raw_df.na.drop()

#df = raw_df.na.drop(how='any')

#df = raw_df.na.drop(how='any', subset=['email'])

#df = raw_df.na.fill('Not Provided')

#df = raw_df.na.fill('missing value', ['customer_id', 'email'])

'''
#Imputer fills null values by taking in columns with nulls values and providing output columns with 
#filled nulls
imputer = Imputer(
    inputCols=['discount_pct'],
    outputCols=['imputed_discount_pct']
).setStrategy('mode')

imputer.fit(raw_df).transform(raw_df).show()
'''

#FILTER OPERATIONS 

#df = raw_df.filter('discount_pct > 10')

#df = raw_df.filter('discount_pct = 20').select(['customer_id', 'total_amount', 'discount_pct'])

#df = raw_df.filter((raw_df['discount_pct'] <= 30) & (raw_df['discount_pct'] >= 20)).select(['customer_id', 'total_amount', 'discount_pct'])

#AGGREGATE FUNCTIONS

#df = raw_df.groupBy('order_id').sum()

#df = raw_df.orderBy('order_id') #automatically sorts by ascending 
#df = raw_df.orderBy(col('order_id').desc()) 
#or
#df = raw_df.sort(col('order_id').desc())

#df = raw_df.groupBy('category').sum()

#df = raw_df.groupBy('category').mean()

#df = raw_df.groupBy('quantity').count()

df = raw_df.groupBy('category').max()

df.show()
print(df.count())