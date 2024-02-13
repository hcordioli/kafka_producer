from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import time
from time import sleep

#current_dateTime = datetime(2024, 2, 6,0,0) + timedelta(minutes=0)
#current_dateTime_int= int(current_dateTime.strftime("%Y%m%d%H%M%S"))

topic = "salestxs"

# Initiate the producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Read json file with the sales transactions
# Opening JSON file
with open('../pinot_tutorial/shopping_trends_updated_online.json', 'r') as file:
    # Iterate over each line in the file
    for line in file:
        # Strip any trailing whitespace and parse the line as JSON
        data = json.loads(line.strip())

        # Add the timestamp
        #data['purchase_time'] = int(time.time() * 1e3)
        data['purchase_time'] = int(datetime.utcnow().timestamp() * 1000)
        
        # Now 'data' contains the JSON object for each line
        # You can access individual fields like this:
        print(data['customer_id'])  # Example usage
        
        future = producer.send(
            topic, 
            data
        )
        result = future.get(timeout=60)
        sleep(5)
        
    # Closing file
    file.close()