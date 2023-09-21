#! /usr/bin/bash

gnome-terminal -- python3 ./recommendation_engine/real_time_recommendation/real_time_recommender/real_time_recommender.py &

sleep 10

gnome-terminal -- python3 ./recommendation_engine/real_time_recommendation/event_processor_mongo/event_processor_mongo.py &

sleep 10

gnome-terminal -- python3 ./recommendation_engine/real_time_recommendation/event_producer/event_producer.py &

sleep 10

gnome-terminal -- node ./server/index.js &

exit