from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import json

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
        temporary_user_preferences = self._realtime_user_preferences_collection.find_one({"session_id": session_id})

        # print(track_features)

        if track_features:
            current_track_acousticness = track_features.get("acousticness")
            current_track_beat_strength = track_features.get("beat_strength")
            current_track_bounciness = track_features.get("bounciness")
            current_track_danceability = track_features.get("danceability")
            current_track_dyn_range_mean = track_features.get("dyn_range_mean")
            current_track_energy = track_features.get("energy")
            current_track_flatness = track_features.get("flatness")
            current_track_instrumentalness = track_features.get("instrumentalness")
            current_track_liveness = track_features.get("liveness")
            current_track_loudness = track_features.get("loudness")
            current_track_mechanism = track_features.get("mechanism")
            current_track_organism = track_features.get("organism")
            current_track_speechiness = track_features.get("speechiness")
            current_track_tempo = track_features.get("tempo")
            current_track_valence = track_features.get("valence")
            current_track_acoustic_vector_0 = track_features.get("acoustic_vector_0")
            current_track_acoustic_vector_1 = track_features.get("acoustic_vector_1")
            current_track_acoustic_vector_2 = track_features.get("acoustic_vector_2")
            current_track_acoustic_vector_3 = track_features.get("acoustic_vector_3")
            current_track_acoustic_vector_4 = track_features.get("acoustic_vector_4")
            current_track_acoustic_vector_5 = track_features.get("acoustic_vector_5")
            current_track_acoustic_vector_6 = track_features.get("acoustic_vector_6")
            current_track_acoustic_vector_7 = track_features.get("acoustic_vector_7")

            # print(acousticness,beat_strength,bounciness,danceability,dyn_range_mean,energy,flatness,instrumentalness,key,liveness,loudness,mechanism,organism,speechiness,tempo,time_signature,valence,acoustic_vector_0,acoustic_vector_1,acoustic_vector_2,acoustic_vector_3,acoustic_vector_4,acoustic_vector_5,acoustic_vector_6,acoustic_vector_7)
        print(temporary_user_preferences)
        if temporary_user_preferences:
            user = {}
            user["session_id"] = session_id
            user["acousticness"] = (temporary_user_preferences.get("acousticness") + current_track_acousticness) / 2
            user["beat_strength"] = (temporary_user_preferences.get("beat_strength") + current_track_beat_strength) / 2
            user["bounciness"] = (temporary_user_preferences.get("bounciness") + current_track_bounciness) / 2
            user["danceability"] = (temporary_user_preferences.get("danceability") + current_track_danceability) / 2
            user["dyn_range_mean"] = (temporary_user_preferences.get("dyn_range_mean") + current_track_dyn_range_mean) / 2
            user["energy"] = (temporary_user_preferences.get("energy") + current_track_energy) / 2
            user["flatness"] = (temporary_user_preferences.get("flatness") + current_track_flatness) / 2
            user["instrumentalness"] = (temporary_user_preferences.get("instrumentalness") + current_track_instrumentalness) / 2
            user["liveness"] = (temporary_user_preferences.get("liveness") + current_track_liveness) / 2
            user["loudness"] = (temporary_user_preferences.get("loudness") + current_track_loudness) / 2
            user["mechanism"] = (temporary_user_preferences.get("mechanism") + current_track_mechanism) / 2
            user["organism"] = (temporary_user_preferences.get("organism") + current_track_organism) / 2
            user["speechiness"] = (temporary_user_preferences.get("speechiness") + current_track_speechiness) / 2
            user["tempo"] = (temporary_user_preferences.get("tempo") + current_track_tempo) / 2
            user["valence"] = (temporary_user_preferences.get("valence") + current_track_valence) / 2
            user["acoustic_vector_0"] = (temporary_user_preferences.get("acoustic_vector_0") + current_track_acoustic_vector_0) / 2
            user["acoustic_vector_1"] = (temporary_user_preferences.get("acoustic_vector_1") + current_track_acoustic_vector_1) / 2
            user["acoustic_vector_2"] = (temporary_user_preferences.get("acoustic_vector_2") + current_track_acoustic_vector_2) / 2
            user["acoustic_vector_3"] = (temporary_user_preferences.get("acoustic_vector_3") + current_track_acoustic_vector_3) / 2
            user["acoustic_vector_4"] = (temporary_user_preferences.get("acoustic_vector_4") + current_track_acoustic_vector_4) / 2
            user["acoustic_vector_5"] = (temporary_user_preferences.get("acoustic_vector_5") + current_track_acoustic_vector_5) / 2
            user["acoustic_vector_6"] = (temporary_user_preferences.get("acoustic_vector_6") + current_track_acoustic_vector_6) / 2
            user["acoustic_vector_7"] = (temporary_user_preferences.get("acoustic_vector_7") + current_track_acoustic_vector_7) / 2

            self._realtime_user_preferences_collection.update_one({'session_id': session_id}, {'$set': user})
            print("Got", temporary_user_preferences)
        else:
            temporary_user_preferences = {
                "session_id": session_id,
                "acousticness": current_track_acousticness,
                "beat_strength": current_track_beat_strength,
                "bounciness": current_track_bounciness,
                "danceability": current_track_danceability,
                "dyn_range_mean": current_track_dyn_range_mean,
                "energy": current_track_energy,
                "flatness": current_track_flatness,
                "instrumentalness": current_track_instrumentalness,
                "liveness": current_track_liveness,
                "loudness": current_track_loudness,
                "mechanism": current_track_mechanism,
                "organism": current_track_organism,
                "speechiness": current_track_speechiness,
                "tempo": current_track_tempo,
                "valence": current_track_valence,
                "acoustic_vector_0": current_track_acoustic_vector_0,
                "acoustic_vector_1": current_track_acoustic_vector_1,
                "acoustic_vector_2": current_track_acoustic_vector_2,
                "acoustic_vector_3": current_track_acoustic_vector_3,
                "acoustic_vector_4": current_track_acoustic_vector_4,
                "acoustic_vector_5": current_track_acoustic_vector_5,
                "acoustic_vector_6": current_track_acoustic_vector_6,
                "acoustic_vector_7": current_track_acoustic_vector_7,
            }
            self._realtime_user_preferences_collection.insert_one(temporary_user_preferences)
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
