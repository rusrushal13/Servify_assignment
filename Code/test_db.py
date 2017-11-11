import pymysql, json
from kafka import KafkaProducer
from kafka import KafkaConsumer

db = pymysql.connect(host="52.66.79.237",port=3306,user="candidate",passwd="asdfgh123",db="servify_assignment")
cursor = db.cursor()
cursor.execute("SELECT * FROM consumer;")
print("consumer: " + str(cursor.rowcount))
consumer_results = cursor.fetchall()

s = "SELECT * FROM consumer_product;"
cursor.execute(s)
print("consumer_product: " + str(cursor.rowcount))
consumer_product_results = cursor.fetchall()

s = "SELECT * FROM consumer_servicerequest;"
cursor.execute(s)
print("consumer_servicerequest: " + str(cursor.rowcount))
consumer_servicerequest_results = cursor.fetchall()

s = "SELECT * FROM sold_plan;"
cursor.execute(s)
print("sold_plan: " + str(cursor.rowcount))
sold_plan_results = cursor.fetchall()

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')
for row in consumer_results:
    producer.send('consumer-topic', json.dumps(row, default=str).encode())

for row in consumer_product_results:
    producer.send('consumer_product-topic', json.dumps(row, default=str).encode())

for row in consumer_servicerequest_results:
    producer.send('consumer_servicerequest-topic', json.dumps(row, default=str).encode())

for row in sold_plan_results:
    producer.send('sold_plan-topic', json.dumps(row, default=str).encode())

db.close()