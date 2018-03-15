from pyspark import SparkContext
distFile = SparkContext().textFile("Datacleaned/yellow_2014.csv")
def myMapFunc(line):
	line = line.split(",")
	temp = float(line[13])
	if temp<15:
		return('1',1)
	if (temp>=15 and temp<40):
		return('2',1)
	if (temp>=40 and temp<70):
		return('3',1)
	if (temp>=70 and temp<100):
		return('4',1)
	if temp>=100 :
		return('5',1)
pairs = distFile.map(myMapFunc)
# counts = pairs.reduceByKey(lambda x : (x[0],sum(x[1:])))
pairs.saveAsTextFile('result.txt')