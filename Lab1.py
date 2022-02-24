import sys
from pyspark import SparkConf, SparkContext


conf = SparkConf()
sc = SparkContext(conf=conf)

# Step 1: Find the number of reviews and calculate the average rating for each product
rdd_rev = sc.textFile(sys.argv[1])
rate_pair = rdd_rev.map(lambda a: (eval(a)['asin'], (eval(a)['overall'], 1)))
# print(rate_pair.collect())
sumrate_pair = rate_pair.reduceByKey(lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1]))
# print(sumrate_pair.collect())
avgrate_pair = sumrate_pair.map(lambda a: (a[0], (a[1][1], a[1][0]/a[1][1])))
# print(avgrate_pair.collect())

# Step 2: Create an RDD (asin,price)
rdd_meta = sc.textFile(sys.argv[2])
# Deal with missing data
rdd_meta = rdd_meta.filter(lambda a: "'price': 'NaN'" not in a)
rdd_meta = rdd_meta.filter(lambda a: "'price'" in a)
price_pair = rdd_meta.map(lambda b: (eval(b)['asin'], eval(b)['price']))
# print(price_pair.collect())

# Step 3: Join the pair RDD obtained in Step 1 and the RDD created in Step 2
price_rate_join = price_pair.rightOuterJoin(avgrate_pair)
# print(price_rate_join.collect())

# Step 4: Find the top 15 products with the greatest number of reviews
price_rate_sort = price_rate_join.sortBy(keyfunc=lambda x: x[1][1][0], ascending = False)
# print(price_rate_sort.take(15))

# Step 5: Output the average rating and price for the top 15 products identified in Step 4
result = price_rate_sort.map(lambda x: ("<"+x[0]+"> <"+str(x[1][1][1])+"> <"+str(x[1][0])+">\n")).take(15)
with open(sys.argv[3], mode='w') as f:
    f.writelines(result)
