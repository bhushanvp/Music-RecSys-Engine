import json
from confluent_kafka import Producer
import requests
import time

class SpotifyUserLogsProducer:
    _TOPIC = "spotify-user-logs"
    _KEY = "realtime-logs"
    _SLEEP_INTERVAL = 2
    def __init__(self, config):
        self._producer = Producer(config)

    def _producer_callback(self, err, msg):
        if err:
            print(err)
        else:
            print(msg)

    def produce_data(self):
        while True:
            try:
                _URL = 'http://localhost:3000/data/userlogs'
                with requests.get(_URL, stream=True) as response:
                    if response.status_code == 200:
                        for line in response.iter_content(chunk_size=1024):
                            if line:
                                data = line.decode('utf-8')
                                try:
                                    data_dict = json.loads(data)
                                    session_id = data_dict.pop('session_id')

                                    modified_data = json.dumps(data_dict)

                                    self._producer.produce(
                                        topic=self._TOPIC,
                                        key=session_id,
                                        value=modified_data,
                                        callback=self._producer_callback
                                    )
                                except json.JSONDecodeError as json_error:
                                    print(f"Failed to parse JSON: {json_error}")
                    else:
                        print(f"Failed to fetch data. Status code: {response.status_code}")

                time.sleep(self._SLEEP_INTERVAL)
                
            except requests.RequestException as e:
                print(f"\nError: {e.args}\n")
                print(f"Retrying...")
                time.sleep(self._SLEEP_INTERVAL)

def main():
    config = {
        "bootstrap.servers": "localhost:9092"
    }

    stock_producer = SpotifyUserLogsProducer(config)
    stock_producer.produce_data()

if __name__ == "__main__":
    main()
