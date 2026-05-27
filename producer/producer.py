import json
import time
import requests
from confluent_kafka import Producer
from datetime import datetime

producer = Producer({    
    'bootstrap.servers': 'kafka:29092'
})

def fetch_rates():
    url = "https://api.frankfurter.app/latest?from=USD&to=EUR,GBP,JPY,AUD,CAD,INR,SGD,AED"
    response = requests.get(url)

    if response.status_code != 200 or not response.text:
        print(f"Bad response: {response.status_code}")
        return None
    
    data = response.json()

    message = {
        'timestamp': datetime.now().isoformat(),
        'base': data['base'],
        'date': data['date'],
        'rates': data['rates']
    }

    return message


def main():
    print("Starting currency rate producer...")

    while True:
        message = fetch_rates()

        if message is None:
            time.sleep(10)
            continue
        
        producer.produce('currency_rates', value=json.dumps(message).encode('utf-8'))
        producer.flush()
        print(f"Sent: {message['timestamp']} - USD/EUR: {message['rates']['EUR']}")
        time.sleep(30)

if __name__ == '__main__':
    main()