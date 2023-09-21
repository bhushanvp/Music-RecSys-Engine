import json
import numpy as np
from pyspark.sql import SparkSession
from confluent_kafka import KafkaError, Consumer
import pyspark.sql.functions as F
from sklearn.metrics.pairwise import cosine_similarity

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
                            .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation.track-features") \
                            .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/spotify-realtime-recommendation.track-features") \
                            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
                            .getOrCreate()

        self.kafka_consumer = KafkaConsumer(recommendation_consumer_config, self._KAFKA_TOPIC)

        self.tracks_df = self.spark \
                            .read \
                            .option("uri", "mongodb://127.0.0.1/spotify-realtime-recommendation/track-features") \
                            .format("com.mongodb.spark.sql.connector.MongoTableProvider") \
                            .load()
        
        self.tracks_features = np.array(self.tracks_df.select(
            ["acousticness", "beat_strength", "bounciness", "danceability", "dyn_range_mean", "energy", "flatness", "instrumentalness", "liveness", "loudness", "mechanism", "organism", "speechiness", "tempo", "valence"]
        ).collect())

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

        user_data = json.loads(msg.value().decode('utf-8'))

        user_features = np.array([
            user_data["acousticness"], user_data["beat_strength"],
            user_data["bounciness"], user_data["danceability"],
            user_data["dyn_range_mean"], user_data["energy"],
            user_data["flatness"], user_data["instrumentalness"],
            user_data["liveness"], user_data["loudness"],
            user_data["mechanism"], user_data["organism"],
            user_data["speechiness"], user_data["tempo"],
            user_data["valence"]
        ]).reshape(1, -1)

        similarity_scores = cosine_similarity(self.tracks_features, user_features).squeeze()
        
        similarity_df = self.spark.createDataFrame(
            [(row["_id"], float(similarity_scores[idx])) for idx, row in enumerate(self.tracks_df.collect())], ["_id", "similarity"]
        )

        num_recommendations = 5
        recommended_tracks = similarity_df.orderBy(F.desc("similarity")).limit(num_recommendations)
        
        print(f"Top 5 recommendations for user {session_id}")
        recommended_tracks.show()

        print("\n\n\n")

    def similarity(v1, v2):
        return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

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