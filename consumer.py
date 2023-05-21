from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import json
import pandas as pd
import matplotlib.dates as mdates
import datetime
import numpy as np

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "temperature"

consumer = KafkaConsumer(
    kafka_topic, bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')))

times = []
values = []

plt.ion()  # Enable interactive mode for real-time plotting

fig, ax = plt.subplots()
ax.xaxis.set_major_locator(mdates.AutoDateLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

try:
    for message in consumer:
        message_value = message.value

        time = datetime.datetime.fromtimestamp(message_value["time"])
        value = message_value["temperature"]

        times.append(time)
        values.append(value)

        data_frame = pd.DataFrame({
            "time": times,
            "value": values
        })
        data_frame["time"] = pd.to_datetime(data_frame["time"])
        print(data_frame)
        plt.clf()
        plt.plot(data_frame["time"], data_frame["value"],
                 label=f"{kafka_topic}", color="red")
        plt.xlabel("Time")
        plt.ylabel("Temperature")
        plt.title("Real-Time Temperature")
        plt.legend()
        plt.xticks(rotation=45)  # Rotate x-axis labels for better visibility
        plt.gcf().autofmt_xdate()  # Auto-format x-axis tick labels

        # Display number of data points on the graph
        num_points = len(data_frame)
        plt.text(0.95, 0.2, f"Number of Points: {num_points}",
                 transform=ax.transAxes, ha='right', va='top')
        plt.text(0.95, 0.3, f"Average: {np.mean(data_frame['value'])}",
                 transform=ax.transAxes, ha='right', va='top')
        plt.pause(0.01)

except KeyboardInterrupt:
    pass

consumer.close()
