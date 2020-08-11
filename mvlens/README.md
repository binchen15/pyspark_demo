spark tutorial using ml-100k dataset.

dataset: MovieLens100k. 

	https://grouplens.org/datasets/movielens/100k/

storage: hadoop hdfs
			"hdfs://localhost:9000/user/binchen/ml-100k/u.data"
			 $ hdfs dfs -ls /user/binchen/ml-100k
				/user/binchen/ml-100k/u.data	
		 local file system
		 	ml-100k/u.item  (local)

tasks:
	1. find worst movies using RDD
	2. find best movies using DataFrame
    3. Recommend movies to a user using MLLib.ALS algorithm

to run using spark-submit or pyspark python module:

```
	$ spark-submit worst_movie_rdd.py
```

or

```
	$ pip install numpy pyspark 
	$ python best_movie_dataframe.py
	$ python recommend_movie_mllib.py 
```
