# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf, array
# from pyspark.sql.types import DoubleType
# from pyspark.ml.linalg import Vectors

# spark = SparkSession.builder \
#     .appName("MongoDBIntegration") \
#     .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation.realtime-user-profiles") \
#     .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation/realtime-user-profiles") \
#     .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
#     .getOrCreate()

# user_profiles_df = spark.read.format("com.mongodb.spark.sql.connector.MongoTableProvider").load()


# track_features_list = ["_id", "acousticness", "beat_strength", "bounciness", "danceability", "dyn_range_mean", "energy", "flatness", "instrumentalness", "liveness", "loudness", "mechanism", "organism", "speechiness", "tempo", "valence", "acoustic_vector_0", "acoustic_vector_1", "acoustic_vector_2", "acoustic_vector_3", "acoustic_vector_4", "acoustic_vector_5", "acoustic_vector_6", "acoustic_vector_7"]

# user_features_list = ["acousticness", "beat_strength", "bounciness", "danceability", "dyn_range_mean", "energy", "flatness", "instrumentalness", "liveness", "loudness", "mechanism", "organism", "speechiness", "tempo", "valence", "acoustic_vector_0", "acoustic_vector_1", "acoustic_vector_2", "acoustic_vector_3", "acoustic_vector_4", "acoustic_vector_5", "acoustic_vector_6", "acoustic_vector_7"]

# tracks_df = tracks_df.select(track_features_list)

# user_profile_df = user_profiles_df.filter(col("session_id")=="0_00010fc5-b79e-4cdf-bc4c-f140d0f99a3a")

# user_profile_df = user_profile_df.select(user_features_list).first()


import json
import numpy as np
from pyspark.sql import SparkSession
from confluent_kafka import KafkaError, Consumer
from pyspark.sql.functions import col, udf, array, expr
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import StructType, StructField, StringType, FloatType

class KafkaConsumer:
    def __init__(self, config, topic):
        self._consumer = Consumer(config)
        self._topic = topic

    def subscribe(self):
        self._consumer.subscribe([self._topic])

    def poll(self, timeout):
        try:
            return self._consumer.poll(timeout)
        except Exception as e:
            print(f"Error in Kafka poll: {e}")

    def close(self):
        try:
            self._consumer.close()
        except Exception as e:
            print(f"Error closing Kafka consumer: {e}")

class SparkApp:
    def __init__(self, recommendation_consumer_config):
        self._KAFKA_TOPIC = "spotify-realtime-recommender"
        self.spark = SparkSession.builder \
                            .appName("Real-time Music Recommender") \
                            .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation.realtime-user-profiles") \
                            .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation.realtime-user-profiles") \
                            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
                            .getOrCreate()

        self.kafka_consumer = KafkaConsumer(recommendation_consumer_config, self._KAFKA_TOPIC)
        
        self.user_profiles_df = self.spark.readStream \
                                    .format("mongodb") \
                                    .option("uri", "mongodb://127.0.0.1:27017/spotify-realtime-recommendation.realtime-user-profiles") \
                                    .load()
        schema = StructType([
            StructField("session_id", StringType(), True),
            StructField("acousticness", FloatType(), True),
            StructField("beat_strength", FloatType(), True),
            StructField("bounciness", FloatType(), True),
            StructField("danceability", FloatType(), True),
            StructField("dyn_range_mean", FloatType(), True),
            StructField("energy", FloatType(), True),
            StructField("flatness", FloatType(), True),
            StructField("instrumentalness", FloatType(), True),
            StructField("liveness", FloatType(), True),
            StructField("loudness", FloatType(), True),
            StructField("mechanism", FloatType(), True),
            StructField("organism", FloatType(), True),
            StructField("speechiness", FloatType(), True),
            StructField("tempo", FloatType(), True),
            StructField("valence", FloatType(), True),
            StructField("acoustic_vector_0", FloatType(), True),
            StructField("acoustic_vector_1", FloatType(), True),
            StructField("acoustic_vector_2", FloatType(), True),
            StructField("acoustic_vector_3", FloatType(), True),
            StructField("acoustic_vector_4", FloatType(), True),
            StructField("acoustic_vector_5", FloatType(), True),
            StructField("acoustic_vector_6", FloatType(), True),
            StructField("acoustic_vector_7", FloatType(), True)
        ])
        # self.tracks_df = self.spark.readStream \
        #                             .format("mongodb") \
        #                             .option("uri", "mongodb://127.0.0.1:27017/spotify-realtime-recommendation.track-features") \
        #                             .load()
        self.tracks_df = self.spark.read.option("uri", "mongodb://127.0.0.1/spotify-realtime-recommendation/track-features").format("com.mongodb.spark.sql.connector.MongoTableProvider").load()

    def consume_realtime_logs(self):
        self.kafka_consumer.subscribe()
        try:
            while True:
                msg = self.kafka_consumer.poll(1.0)

                if msg is None:
                    print("Polling....")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached end of partition")
                    else:
                        print(msg.error())
                else:
                    try:
                        self._process_message(msg)
                    except Exception as e:
                        print(f"Error processing message: {e}")

        except KeyboardInterrupt:
            self.kafka_consumer.close()
            print("Shutting down...")

    def _process_message(self, msg):
        session_id = msg.key().decode('utf-8')

        user_features = json.loads(msg.value().decode('utf-8'))

        print(f"Processing message for session ID: {session_id} ->", user_features)

        self.tracks_df.show(1)

        print("\n\n\n")

def main():
    recommendation_consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "console-consumer-97531",
        "auto.offset.reset": "latest"
    }

    spark_app = SparkApp(recommendation_consumer_config)

    spark_app.consume_realtime_logs()

if __name__=="__main__":
    main()