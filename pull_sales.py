from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta

topic = "salestxs"

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
    topic,
    #group_id='my-group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),    # ascii
)

class LastMinuteBuffer:
    def __init__(self):
        self.buffer = []
    
    def update(self,tx):
        # Calculate timestamp for one minute ago
        one_minute_ago = datetime.utcnow() - timedelta(minutes=1)
        one_minute_ago_timestamp = int(one_minute_ago.timestamp() * 1000)  # Convert to milliseconds
  
        self.buffer = [ msg for msg in self.buffer  if msg['purchase_time'] >= one_minute_ago_timestamp]
        self.buffer.append(tx)
        lm_sale = sum([ msg['purchase_amount_usd'] for msg in self.buffer ])
        
        print (len(self.buffer),"=>",lm_sale)

last_minute_buffer = LastMinuteBuffer()
 
# Consume messages and filter based on purchase_time
for message in consumer:
    salestx = message.value
    purchase_time = salestx['purchase_time']
    
    # Check if purchase_time is within the last minute
    last_minute_buffer.update(salestx)