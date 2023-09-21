from confluent_kafka import Consumer, KafkaError, Producer
from pymongo import MongoClient
import json

class KafkaProducer:
    _TOPIC = "spotify-realtime-recommender"
    _KEY = "generate-recommendation"

    def __init__(self, config):
        self._producer = Producer(config)

    def _producer_callback(self, err, msg):
        if err:
            print(err)
        else:
            print("Produced :", msg)

    def produce_data(self, key, msg):
        user_data = json.dumps(msg)
        self._producer.produce(
            topic=self._TOPIC,
            key=key,
            value=user_data,
            callback=self._producer_callback
        )

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

class MongoDBClient:
    def __init__(self, host, port, db_name):
        try:
            self._client = MongoClient(host, port)
            self._db = self._client.get_database(db_name)
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")

    def get_collection(self, collection_name):
        try:
            return self._db.get_collection(collection_name)
        except Exception as e:
            print(f"Error getting MongoDB collection: {e}")

class SpotifyUserLogsConsumer:
    _KAFKA_TOPIC = "spotify-user-logs"
    _MONGODB_HOST = "localhost"
    _MONGODB_PORT = 27017
    _MONGODB_DB = "spotify-realtime-recommendation"
    _MONGODB_REALTIME_USER_PROFILES_COLLECTION = "realtime-user-profiles"
    _MONGODB_CURRENT_TRACK_FEATURES = "track-features"

    def __init__(self, consumer_config, producer_config):
        self.kafka_consumer = KafkaConsumer(consumer_config, self._KAFKA_TOPIC)
        self.kafka_producer = KafkaProducer(producer_config)

        self.mongodb_client = MongoDBClient(self._MONGODB_HOST, self._MONGODB_PORT, self._MONGODB_DB)
        self._realtime_user_profiles_collection = self.mongodb_client.get_collection(self._MONGODB_REALTIME_USER_PROFILES_COLLECTION)
        self._current_track_features = self.mongodb_client.get_collection(self._MONGODB_CURRENT_TRACK_FEATURES)

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
        value = json.loads(msg.value().decode('utf-8'), parse_float=float, parse_int=int)

        track_id = value.get("track_id_clean")

        if (value.get("skip_2")==True) or (value.get("skip_1")==True):
            return

        current_track_features = self._current_track_features.find_one({"track_id": track_id})
        temporary_user_profile = self._realtime_user_profiles_collection.find_one({"session_id": session_id})

        if current_track_features is None:
            return

        if temporary_user_profile:
            new_temporary_user_profile = {"session_id": session_id}

            for key in temporary_user_profile:
                if key != "session_id" and key != "_id":
                    new_temporary_user_profile[key] = (temporary_user_profile[key] * 1.5 + current_track_features[key] * 0.5) / 2

            try:
                self._realtime_user_profiles_collection.update_one({'session_id': session_id}, {'$set': new_temporary_user_profile})

                new_temporary_user_profile.pop("session_id")

                self.kafka_producer.produce_data(session_id, new_temporary_user_profile)
                print("Got")
            except Exception as e:
                print(f"Error updating MongoDB: {e}")

        else:
            temporary_user_profile = {"session_id": session_id}

            excluded_keys = ["session_id", "_id", "track_id", "mode", "key", "time_signature", "duration", "release_year", "us_popularity_estimate"]

            for key in current_track_features:
                if key not in excluded_keys:
                    temporary_user_profile[key] = current_track_features[key]

            try:
                self._realtime_user_profiles_collection.insert_one(temporary_user_profile)

                temporary_user_profile.pop("session_id")

                self.kafka_producer.produce_data(session_id, temporary_user_profile)
                print("Hit")
            except Exception as e:
                print(f"Error inserting into MongoDB: {e}")

def main():
    mongo_consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "console-consumer-97531",
        "auto.offset.reset": "latest"
    }

    gen_rec_producer_config = {
        "bootstrap.servers": "localhost:9092"
    }

    stock_consumer = SpotifyUserLogsConsumer(mongo_consumer_config, gen_rec_producer_config)
    stock_consumer.consume_realtime_logs()

if __name__ == "__main__":
    main()
