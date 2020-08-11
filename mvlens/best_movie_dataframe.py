from pyspark.sql import SparkSession, Row, functions

def movie_names():
    ans = {}  # ans[movie_id] = "movie_name"
    for line in open("ml-100k/u.item", encoding = "ISO-8859-1"):
        fields = line.split("|")
        ans[fields[0]] = fields[1]
    return ans

def movie_rating(line):
    fields = line.split("\t")
    return Row(id=fields[1], rating=float(fields[2]))


if __name__ == "__main__":
    names = movie_names()
    spark = SparkSession.builder.appName("BestMovies").getOrCreate()
    rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/binchen/ml-100k/u.data")
    ratings = rdd.map(movie_rating)  # and RDD of Row objects
    df = spark.createDataFrame(ratings)
    ratings = df.groupBy("id").avg('rating') # ["id", "avg(rating)"]
    counts = df.groupBy("id").count()
    combo = counts.join(ratings, "id")  # ['id','count','avg(rating)']
    bests = combo.orderBy("avg(rating)", ascending=False).take(10)

    for movie in bests:
        print("{:50s}\t{}\t{}".format(names[movie[0]], movie[1], movie[2]))

    spark.stop()
