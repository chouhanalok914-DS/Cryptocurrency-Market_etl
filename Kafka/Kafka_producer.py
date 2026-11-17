"""
==================================================================
KAFKA PRODUC -- CoinGecko Live cryptocurrency data
==================================================================

FLOW :-  
CoinGecko API ->  python producer -> Apache kafka topic

1. Fetch cryptocurrency market data from CoinGeko api
2. convert python data into JSON format and then into bytes
3. Send each cryptocurrency as a seperate message to kafka.
4. Wait 30 seconds and repeat the entire process. 

IMPORTANT CONCEPTS :
-- API :- Used to fetch cruptocurrency market data.
-- Producer :- A program that send message to kafka.
-- Broker :-  The Kafka server that receives and stored message.
-  Topics :- A logical channel where kafka message are stored 
-  Message :-  A single records containing cryptocurrecy information.
-  JSON :-  (javascript object notation ) - A format usedd ot represent structured data.
-  Serialization :- Converting data into a format suitable for transmission.

INSTALL DEPENDENCIES:-

python -m pip install kafka - python reequests

RUN  THE FILE:-

python kafka/kafaka_producer.py

Requirements :- 
1. kafka must be running at localhost:9092
2. The kafka topic "crypto-prices" must exist. or the broker must be cofigured to create topics automatically
=============================================================================================================
"""

# ============================================================================
# IMPORT REQUIRED LIBRATIES
# ============================================================================

# converts python dictionaries  into JSON String.
# Kafka message need to be serialized into bytes before sending 
import json

# Provides the sleep() function.
# We use it to pause the program between API requests.
import time

# Send HTTP request to the coingecko REST[Repres entation state transfer] 
#  we use it to fetch the latest cryptocurrency market data
import requests                                       

#  A class provided by kafka-python
# It allows our python program to publish into byutes sending 
from kafka import KafkaProducer


# ======================================================================
# PART 2 CONFIGURATION 
# ======================================================================

#  Address of the kafka broker
#  localhost means kafka is running on the same computer 
#  port 

# KAFKA_BROKER = "localhost:9092"
KAFKA_BROKER = "localhost:29092"

# name of the kafka topic
# A topic is a logical channel that stored related message.
# Our cryptocurrency records will be published to this topic 

KAFKA_TOPIC = 'crypto-prices'

KAFKA_INTERVAL = 30
# Number of seconds to wait between sucessive API requests 
# After fetching and sending batch , the program sleep.
# for 30 seconds before sending theh next batch

FETCH_INTERVAL = 30

# Coingecko's public REST API endpoints for market information 
# This endpoints returns information such as coin name, price,
# market capitilization and 25 hours price changes

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

# ===============================================================================
# PART 3 : CONNECT TO KAFKA
# ===============================================================================

# KafkaProducer create a producer object
# This object manages the connection and send message to kafka
producer = KafkaProducer(
    # bootsrap servers tells the producer where to initially
    bootstrap_servers = KAFKA_BROKER,
    value_serializer = lambda data : json.dumps(data).encode("utf-8"),
)


print(f"connected to kafka at {KAFKA_BROKER}")
print(f"Target kafka topic {KAFKA_TOPIC}")
print(f"Fetching data every {FETCH_INTERVAL}senconds\n")

# ===============================================================================
# PART 4 : FETCH DATA FORM COINGECKO API
# ===============================================================================

def fetch_coins():

    """
     Fetch cryptocurrency market data from coinGecko

     Returns:
     A list of dictionries , with each disctinary representing 
     one cryptocurreny 
    """

    params = {
        "vs_currency":"usd",
        "order":"market_cap_desc",
        "per_page":100,
        "page":1,
        "sparkline":False,  # Do not include sparkline price-history data # This reduces unnecessary response data 
        "price_change_percentage":"24h",
        }

    # Send an HTTP GET request to coinGecko API 
    # params automatically adds the query parameter tp the URL
    # timeout=15 means the request will not wait indefinitely.
    response=requests.get(
        API_URL,
        params=params,
        timeout=15
    )

   # Raise an exection for HTTP errors , such as 429 or 500
   # This prevents us from processing an error message 
    response.raise_for_status() 

   # Convert API's JSON response into python object 
   # a list of dictionaries , one dictionary per cryptocurreny 
    coins=response.json()

    return coins

# =========================================================
# PART 5: SEND CRYPTOCURRENCY DATA TO KAFKA 
# =========================================================                          
def send_to_kafka(coins):


    for coin in coins:
        producer.send(

            KAFKA_TOPIC,
            key = coin["id"].encode("utf-8"),
            value = coin
        )

    producer.flush()

    print(
        f"sent{len(coins)} coins to kafka topic {KAFKA_TOPIC}"
    )

# ===============================================================================
# PART 6 : MAIN LOOP 
# Run forever : fetch -> send -> wait 30 seconds -> repeat
# ctrl  +c to stop
# ===============================================================================

batch = 0
print("Producer started......")
while True:
    batch += 1
    print(f"\n [Batch # {batch}]")

    # step 1 fetch from API

    print("Fetching from coingecko API")
    coins = fetch_coins()
    print(f"Got {len(coins)} coins")

    # step 2 : show a quick preview/show currency (top3)
    print("Preview (top 3):")
    for coin in coins[:3]:
        print(f"{coin['name']:15s}${coin["current_price"]:>12,.2f}")
        f"({coin["price_change_percentage_24h"]:+.2f}%24h)"
    # step 3 : send all coins to kafka 
    send_to_kafka(coins)

    # step 4 : wait before next batch
    print(f"sleeping {FETCH_INTERVAL}S.....")

    time.sleep(FETCH_INTERVAL)    









































# https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=1&sparkline=false&price_change_percentage=24h













