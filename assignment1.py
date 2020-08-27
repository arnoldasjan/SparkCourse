from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


def parseLines(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return customerID, amount


lines = sc.textFile("customer-orders.csv")
rdd = lines.map(parseLines)
total = rdd.reduceByKey(lambda x, y: x + y)
sortedtotal = total.map(lambda x: (x[1], x[0])).sortByKey()
results = sortedtotal.collect()


for result in results:
    print(str(result[1]) + "\t{:.2f}".format(result[0]))
