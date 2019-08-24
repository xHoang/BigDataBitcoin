import pyspark, re

sc = pyspark.SparkContext()

def cleaned_vin(line):
	try:
		fields = line.split(',')
		if len(fields) != 3:
			return False
		float(fields[2])
		return True
	except:
		return False

def clean_vout(line):
	try:
		fields = line.split(',')
		if len(fields) != 4 or fields[3] != "{18iEz617DoDp8CNQUyyrjCcC7XCGDf5SVb}":
			return False
		float(fields[1])
		float(fields[2])
		return True
	except:
		return False


def clean_vout_no_wiki(line):
		try:
			fields = line.split(',')
			if len(fields) != 4:
				return False
			float(fields[1])
			float(fields[2])
			return True
		except:
			return False



vout = sc.textFile("/data/bitcoin/vout.csv")
cleaned_vout = vout.filter(clean_vout).map(lambda l: l.split(","))
vout_join = cleaned_vout.map(lambda l: (l[0], ((l[1]), float(l[2]), l[3])))


vin = sc.textFile("/data/bitcoin/vin.csv")
cleaned_vin = vin.filter(cleaned_vin).map(lambda a: a.split(","))
vin_join = cleaned_vin.map(lambda l: (l[1], (l[0])))

#Joined (tx_hash, (vin(tx_id), vout(value, n, publicKey)))
joined_data = vin_join.join(vout_join)

hash_vout_KV = joined_data.map(lambda b: ((b[1][0]),"667"))

#vout((tx_hash), (value, n,publicKey))
cleaned_vout_no_wiki = vout.filter(clean_vout_no_wiki).map(lambda c: c.split(',')).map(lambda c: ((c[0]), (c[1], c[2], c[3])))

second_join = hash_vout_KV.join(cleaned_vout_no_wiki)
#second_join.saveAsTextFile("pleaseworkx")
second_KV = second_join.map(lambda d: (d[1][1][2], float(d[1][1][1])))
second_join_reduced = second_KV.reduceByKey(lambda a,b: a+b)
second_join_reduced.saveAsTextFile("pleasework99")
#part4 = second_join_reduced.sortBy(lambda x: -x[1])
#sc.parallelize(part4).saveAsTextFile("part4xdd99")
