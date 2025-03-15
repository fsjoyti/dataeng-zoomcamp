import csv
import json
from time import time

import pandas as pd
from kafka import KafkaProducer


def send_data_to_kafka(row, topic_name, producer):
    message = row.to_dict()
    producer.send(topic_name, value=message)
    return row


def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    csv_file = (
        "data/green_tripdata_2019-10.csv"  # change to your CSV file path if needed
    )

    df = pd.read_csv(csv_file)

    df = df.loc[
        :,
        [
            "lpep_pickup_datetime",
            "lpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance",
            "tip_amount",
        ],
    ]

    # drop rows with missing values
    # replace missing passenger_count values with 0

    # if passenger_count is na replace with 0
    df["passenger_count"] = df["passenger_count"].fillna(0)
    # convert passenger_count to int
    df["passenger_count"] = df["passenger_count"].astype(int)
    # if trip_distance is na replace with 0
    df["trip_distance"] = df["trip_distance"].fillna(0)

    # if tip_amount is na replace with 0
    df["tip_amount"] = df["tip_amount"].fillna(0)

    print(df.head())
    # Convert the DataFrame to a list of dictionaries
    data = df.to_dict(orient="records")

    topic_name = "green_trips"
    t0 = time()
    # Send each row to Kafka
    for row in data:
        producer.send(topic_name, value=row)

    producer.flush()
    t1 = time()
    took = t1 - t0
    print(f"Sending data took {took} seconds")
    print("Data sent to Kafka successfully!")
    producer.close()

    # t0 = time()

    # df = df.apply(lambda row: send_data_to_kafka(row, topic_name, producer), axis=1)

    # producer.flush()

    # t1 = time()
    # took = t1 - t0

    # print(took)

    # # Make sure any remaining messages are delivered
    # producer.flush()
    # producer.close()


if __name__ == "__main__":
    main()
