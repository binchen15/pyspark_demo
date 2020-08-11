from pyspark import SparkContext, SparkConf

def movie_names():
    ans = {}  # ans[movie_id] = "movie_name"
    for line in open("ml-100k/u.item", encoding = "ISO-8859-1"):
        fields = line.split("|")
        ans[fields[0]] = fields[1]
    return ans

def movie_rating(line):
    fields = line.split("\t")
    return (fields[1], (float(fields[2]), 1)) # (id, (rate, 1))


if __name__ == "__main__":
    conf = SparkConf().setAppName("Worst Movie")
    sc = SparkContext(conf=conf)
    names = movie_names()
    rdd = sc.textFile("hdfs://localhost:9000/user/binchen/ml-100k/u.data")
    ratings = rdd.map(movie_rating)
    #print(ratings.take(2))
    scores = ratings.reduceByKey(lambda val1, val2: (val1[0] + val2[0], val1[1] + val2[1]))
    avgs = scores.mapValues(lambda val: val[0]/val[1])
    sortedAvgs = avgs.sortBy(lambda avg: avg[1])
    worsts = sortedAvgs.take(10)
    for movie in worsts:
        print(names[movie[0]], movie[1])
