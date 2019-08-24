import pyspark
import re

sc = pyspark.SparkContext()
#https://www.blockchain.com/btc/address/18iEz617DoDp8CNQUyyrjCcC7XCGDf5SVb
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 4:
            return False
        if fields[3] == "{18iEz617DoDp8CNQUyyrjCcC7XCGDf5SVb}":
            float(fields[1])
            return True
        return False

    except:
        return False


lines = sc.textFile("/data/bitcoin/vout.csv")

clean_lines = lines.filter(is_good_line)
reduced_lines = clean_lines.map(lambda x: x.split(',')).map(lambda x: (x[3], float(x[1]))).reduceByKey(lambda a,b: a+b)
reduced_lines.saveAsTextFile("ransomwareTotalCoins")
