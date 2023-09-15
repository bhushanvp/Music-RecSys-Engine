import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['spotify-realtime-recommendation']
collection = db['track-features']

# Read CSV file into a Pandas DataFrame
df = pd.read_csv("./server/resources/data/track_features/tf_mini.csv")

# Convert DataFrame to a list of dictionaries (one dictionary per row)
data = df.to_dict(orient='records')

# Insert data into MongoDB
collection.insert_many(data)

# Close the MongoDB connection
client.close()
