from confluent_kafka import Producer
import requests

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
        try:
            _URL = 'http://localhost:3000/data/userlogs'
            with requests.get(_URL, stream=True) as response:
                if response.status_code == 200:
                    for line in response.iter_content(chunk_size=1024):
                        if line:
                            data = line.decode('utf-8')
                            # print(data)
                            self._producer.produce(
                                topic=self._TOPIC,
                                key=self._KEY,
                                value=data,
                                callback=self._producer_callback
                            )
                else:
                    print(f"Failed to fetch data. Status code: {response.status_code}")
        except Exception as e:
            print(f"Error: {e}")

def main():
    config = {
        "bootstrap.servers": "localhost:9092"
    }

    stock_producer = SpotifyUserLogsProducer(config)
    stock_producer.produce_data()

if __name__ == "__main__":
    main()
