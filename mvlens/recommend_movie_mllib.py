from pyspark.sql import SparkSession, Row
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import lit


def movie_names():
    ans = {}  # ans[id] = "name"
    for line in open("ml-100k/u.item", encoding="ISO-8859-1"):
        fields = line.split("|")
        ans[int(fields[0])] = fields[1]
    return ans


def movie_rating(line):   # numeric columns for learning algorithm
    fields = line.split("\t")
    return Row(uid=int(fields[0]), id=int(fields[1]), rating=float(fields[2]))


def fake_user_ratings():
    return [
            Row(uid=0, id=50, rating=5.0),
            Row(uid=0, id=172, rating=5.0),
            Row(uid=0, id=133, rating=1.0),
            ]


if __name__ == "__main__":
    spark = SparkSession.builder.appName("BestMovies").getOrCreate()
    names = movie_names()
    fake_ratings = spark.sparkContext.parallelize(fake_user_ratings())
    rdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/binchen/ml-100k/u.data")
    ratings = rdd.map(movie_rating)  # and RDD of Row objects
    ratingsRDD = ratings.union(fake_ratings)  # the training data with fake user (id=0) now
    df = spark.createDataFrame(ratingsRDD).cache()

    for row in fake_user_ratings():
        print(names[row.id], row.rating)

    for row in df.rdd.take(10):
        print(row)

    for row in df.filter("uid = 0").rdd.take(3):
        print(row)

    als = ALS(maxIter=6, regParam=0.01, userCol='uid', itemCol="id",
              ratingCol="rating")

    model = als.fit(df)  # df.cache() impacts performance here

    pop_ratings = df.groupBy('id').count().filter("count > 100")
    pred_features = pop_ratings.select("id").withColumn('uid', lit(0))

    recommends = model.transform(pred_features)
    predictions = recommends.sort(recommends.prediction.desc()).take(20)

    for movie in predictions:
        print("{:50s}\t{}".format(names[movie.id], movie.prediction))

    spark.stop()
