from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, Bucketizer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import explode, array_contains, lit, col

# Where to output the trained model
model_path = "algorithms/predictor_model"

url = "neo4j://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"

def load_data():
    # Get the data from Neo4j
    query = """
        MATCH (m:Movie)
        RETURN m.genres AS genres, m.runtimeMinutes AS runtimeMinutes,
            m.startYear AS startYear, m.averageRating AS averageRating
    """

    raw_data = (
        spark.read.format("org.neo4j.spark.DataSource")
        .option("url", "neo4j://localhost:7687")
        .option("authentication.basic.username", "neo4j")
        .option("authentication.basic.password", "password")
        .option("database", "neo4j")
        .option("query", query)
        .load()
    )

    return raw_data

def prepare_data(df, include_rating = True, all_genres=[]):
    if include_rating:
    # Bucketize averageRating into categories
        bucketizer = Bucketizer(
            splits=[0, 4, 7, 10],  # Low: 0-4, Medium: 4-7, High: 7-10
            inputCol="averageRating",
            outputCol="ratingCategory"
        )
        df = df.withColumn("averageRating", col("averageRating").cast("int"))
        df = bucketizer.setHandleInvalid("skip").transform(df)

    # Bucketize runtimeMinutes into categories
    runtime_bucketizer = Bucketizer(
        splits=[0, 90, 120, 180, 210, 240, float('inf')],  # 'Buckets' of movie lengths
        inputCol="runtimeMinutes",
        outputCol="runtimeCategory"
    )
    df = df.withColumn("runtimeMinutes", col("runtimeMinutes").cast("int"))
    df = runtime_bucketizer.setHandleInvalid("skip").transform(df)

    # Bucketize startYear into categories
    year_bucketizer = Bucketizer(
        splits=[1900, 1925, 1950, 1975, 2000, 2025],
        inputCol="startYear",
        outputCol="yearCategory"
    )
    df = df.withColumn("startYear", col("startYear").cast("int"))
    df = year_bucketizer.setHandleInvalid("skip").transform(df)

    # Convert genres to multi-hot encoding, unless existing list is passed in
    if all_genres:
        genre_list = all_genres
    else:
        genre_exploded = df.select(explode(df.genres).alias("genre")).distinct()
        genre_list = [row["genre"] for row in genre_exploded.collect()]  # Unique

        print(genre_list)

    for genre in genre_list:
        df = df.withColumn(f"genre_{genre}", array_contains(df.genres, genre).cast("double"))

    # Replace nulls in numeric columns
    feature_columns = [f"genre_{genre}" for genre in genre_list] + ["runtimeCategory", "yearCategory"]
    for col_name in feature_columns:
        df = df.withColumn(col_name, col(col_name).cast("double"))
        df = df.fillna({col_name: 0.0})

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    return df

def train_model(vectorized_data):
    training_data = vectorized_data.select("features", "ratingCategory").na.drop()

    # Train-test split
    # train_data, test_data = training_data.randomSplit([0.9, 0.1], seed=42)

    # print(test_data.take(1)[0].asDict())

    # Train model
    nb = NaiveBayes(featuresCol="features", labelCol="ratingCategory", modelType="multinomial")
    model = nb.fit(training_data)

    # Evaluate model
    #predictions = model.transform(test_data)
    #evaluator = MulticlassClassificationEvaluator(
    #    labelCol="ratingCategory", predictionCol="prediction", metricName="accuracy")

    #accuracy = evaluator.evaluate(predictions)
    #print(f"Accuracy: {accuracy * 100:.2f}%")

    return model

if '__main__' == __name__:
    spark = (
        SparkSession.builder.config("neo4j.url", url)
        .config("spark.jars", "file:///C:/Users/zgoos/Downloads/spark-3.5.3-bin-hadoop3/jars/neo4j-spark-connector-5.3.1-s_2.12.jar")
        .getOrCreate()
    )

    print("===== GETTING DATA =====")
    raw_data = load_data()

    print("===== DATA LOADED - NOW TRANSFORMING FOR MODEL =====")
    vectorized_data = prepare_data(raw_data)

    # ======= Training Model =======
    print("===== DATA VECTORIZED - NOW TRAINING MODEL =====")
    model = train_model(vectorized_data)

    model.write().overwrite().save(model_path)