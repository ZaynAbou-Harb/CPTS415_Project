from flask import Flask
from pyspark.sql import SparkSession
from controllers.main_controller import main_controller
from pyspark.ml.classification import NaiveBayesModel

app = Flask(__name__)
app.register_blueprint(main_controller)

url = "bolt://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"

spark = (
    SparkSession.builder
    .config("spark.jars", "file:///C:/Users/eunic/spark-3.5.3-bin-hadoop3/jars/neo4j-connector-apache-spark_2.12-5.3.2_for_spark_3.jar")
    .config("spark.neo4j.url", url)
    .config("spark.neo4j.authentication.basic.username", username)
    .config("spark.neo4j.authentication.basic.password", password)
    .config("spark.neo4j.database", dbname)
    .getOrCreate()
)

prediction_model = NaiveBayesModel.load("algorithms/predictor_model")

app.config['SPARK_SESSION'] = spark
app.config['PREDICTION_MODEL'] = prediction_model

if __name__ == '__main__':
    app.run(debug=True, port=5001)
