from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import datetime
import json
import pytz

class SpotifyUserLogsConsumer:
    _KAFKA_TOPIC = "spotify-user-logs"
    _MONGODB_HOST = "localhost"
    _MONGODB_PORT = 27017
    _MONGODB_DB = "spotify-realtime-recommendation"
    _MONGODB_REALTIME_USER_PREFERENCES_COLLECTION = "realtime-user-preferences"
    _MONGODB_TRACK_FEATURES = "track-features"

    def __init__(self, config):
        self._consumer = Consumer(config)
        self._client = MongoClient(self._MONGODB_HOST, self._MONGODB_PORT)
        self._db = self._client.get_database(self._MONGODB_DB)
        self._realtime_user_preferences_collection = self._db.get_collection(self._MONGODB_REALTIME_USER_PREFERENCES_COLLECTION)
        self._track_features = self._db.get_collection(self._MONGODB_TRACK_FEATURES)

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
        key = msg.key().decode('utf-8')
        value = json.loads(msg.value().decode('utf-8'), parse_float=float, parse_int=int)

        track_id = value.get("track_id_clean")
        session_id = value.get("session_id")

        # print(track_id)

        track_features = self._track_features.find_one({"track_id": track_id})
        temorary_user_preferences = self._realtime_user_preferences_collection.find_one({"session_id": session_id})

        # print(track_features)

        if track_features:
            acousticness = track_features.get("acousticness")
            beat_strength = track_features.get("beat_strength")
            bounciness = track_features.get("bounciness")
            danceability = track_features.get("danceability")
            dyn_range_mean = track_features.get("dyn_range_mean")
            energy = track_features.get("energy")
            flatness = track_features.get("flatness")
            instrumentalness = track_features.get("instrumentalness")
            key = track_features.get("key")
            liveness = track_features.get("liveness")
            loudness = track_features.get("loudness")
            mechanism = track_features.get("mechanism")
            # mode = track_features.get("mode")
            organism = track_features.get("organism")
            speechiness = track_features.get("speechiness")
            tempo = track_features.get("tempo")
            time_signature = track_features.get("time_signature")
            valence = track_features.get("valence")
            acoustic_vector_0 = track_features.get("acoustic_vector_0")
            acoustic_vector_1 = track_features.get("acoustic_vector_1")
            acoustic_vector_2 = track_features.get("acoustic_vector_2")
            acoustic_vector_3 = track_features.get("acoustic_vector_3")
            acoustic_vector_4 = track_features.get("acoustic_vector_4")
            acoustic_vector_5 = track_features.get("acoustic_vector_5")
            acoustic_vector_6 = track_features.get("acoustic_vector_6")
            acoustic_vector_7 = track_features.get("acoustic_vector_7")

            # print(acousticness,beat_strength,bounciness,danceability,dyn_range_mean,energy,flatness,instrumentalness,key,liveness,loudness,mechanism,organism,speechiness,tempo,time_signature,valence,acoustic_vector_0,acoustic_vector_1,acoustic_vector_2,acoustic_vector_3,acoustic_vector_4,acoustic_vector_5,acoustic_vector_6,acoustic_vector_7)
        print(temorary_user_preferences)
        if temorary_user_preferences:
            print("Got", session_id)
        else:
            temorary_user_preferences = {
                "session_id": session_id,
                "acousticness": acousticness,
                "beat_strength": beat_strength,
                "bounciness": bounciness,
                "danceability": danceability,
                "dyn_range_mean": dyn_range_mean,
                "energy": energy,
                "flatness": flatness,
                "instrumentalness": instrumentalness,
                "key": key,
                "liveness": liveness,
                "loudness": loudness,
                "mechanism": mechanism,
                "organism": organism,
                "speechiness": speechiness,
                "tempo": tempo,
                "time_signature": time_signature,
                "valence": valence,
                "acoustic_vector_0": acoustic_vector_0,
                "acoustic_vector_1": acoustic_vector_1,
                "acoustic_vector_2": acoustic_vector_2,
                "acoustic_vector_3": acoustic_vector_3,
                "acoustic_vector_4": acoustic_vector_4,
                "acoustic_vector_5": acoustic_vector_5,
                "acoustic_vector_6": acoustic_vector_6,
                "acoustic_vector_7": acoustic_vector_7,
            }
            self._realtime_user_preferences_collection.insert_one(temorary_user_preferences)
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
