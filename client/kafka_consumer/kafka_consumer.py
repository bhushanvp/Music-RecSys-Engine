from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import json

class SpotifyUserLogsConsumer:
    _KAFKA_TOPIC = "spotify-user-logs"
    _MONGODB_HOST = "localhost"
    _MONGODB_PORT = 27017
    _MONGODB_DB = "spotify-realtime-recommendation"
    _MONGODB_REALTIME_USER_PROFILES_COLLECTION = "realtime-user-profiles"
    _MONGODB_CURRENT_TRACK_FEATURES = "track-features"

    def __init__(self, config):
        self._consumer = Consumer(config)
        self._client = MongoClient(self._MONGODB_HOST, self._MONGODB_PORT)
        self._db = self._client.get_database(self._MONGODB_DB)
        self._realtime_user_profiles_collection = self._db.get_collection(self._MONGODB_REALTIME_USER_PROFILES_COLLECTION)
        self._current_track_features = self._db.get_collection(self._MONGODB_CURRENT_TRACK_FEATURES)

    def consume_realtime_logs(self):
        self._consumer.subscribe([self._KAFKA_TOPIC])

        try:
            while True:
                msg = self._consumer.poll(1.0)

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
            self._consumer.close()
            print("Shutting down...")

    def _process_message(self, msg):
        session_id = msg.key().decode('utf-8')
        value = json.loads(msg.value().decode('utf-8'), parse_float=float, parse_int=int)

        track_id = value.get("track_id_clean")

        if (value.get("skip_2")==True) or (value.get("skip_1")==True):
            return

        current_track_features = self._current_track_features.find_one({"track_id": track_id})
        temporary_user_profile = self._realtime_user_profiles_collection.find_one({"session_id": session_id})

        if current_track_features==None:
            return
        
        if temporary_user_profile:
            new_temporary_user_profile = {}
            new_temporary_user_profile["session_id"] = session_id

            for key in temporary_user_profile:
                if key!="session_id" and key!="_id":
                    new_temporary_user_profile[key] = (temporary_user_profile[key]*1.5+current_track_features[key]*0.5)/2

            self._realtime_user_profiles_collection.update_one({'session_id': session_id}, {'$set': new_temporary_user_profile})
            print("Got")

        else:
            temporary_user_profile = {}
            temporary_user_profile["session_id"] = session_id

            excluded_keys = [ "session_id", "_id", "track_id", "mode", "key", "time_signature", "duration", "release_year", "us_popularity_estimate" ]

            for key in current_track_features:
                if key not in excluded_keys:
                    temporary_user_profile[key] = current_track_features[key]
                    
            self._realtime_user_profiles_collection.insert_one(temporary_user_profile)
            print("Hit")

def main():
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "console-consumer-97531",
        "auto.offset.reset": "latest"
    }
    stock_consumer = SpotifyUserLogsConsumer(config)
    stock_consumer.consume_realtime_logs()

if __name__ == "__main__":
    main()
