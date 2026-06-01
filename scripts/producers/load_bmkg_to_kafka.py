import json
import cloudscraper  
from kafka import KafkaProducer

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
        
    url = "https://api.bmkg.go.id/publik/prakiraan-cuaca?adm4=35.10.05.2006"   
        
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    topic_name = 'bmkg_topic'
    
    try:
        print(f"[*] Mengambil data dari API BMKG memakai Cloudscraper...")
                
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, headers=headers, timeout=15) 
        response.raise_for_status()        
        raw_weather_data = response.json()
        
        print(f"[+] Sukses menembus Cloudflare! Mengirim data mentah ke Kafka Topic: {topic_name}")
        producer.send(
            topic_name,
            value=raw_weather_data
        )
        
        producer.flush()
        print("[+] Ingest ke Kafka Berhasil!")

    except Exception as e:
        print(f"[-] Error saat ingest: {e}")
        raise e 

    finally:                
        producer.close(timeout=10)

if __name__ == "__main__":
    run_producer()