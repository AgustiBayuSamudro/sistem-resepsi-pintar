import json
import os
import time
from kafka import KafkaProducer
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ==================== KOREKSI KONFIGURASI PATH & NETWORK ====================
KAFKA_BROKER = 'kafka:9092'  # Network service Docker Kafka
TOPIC_UNDANGAN = 'undangan_topic'
TOPIC_TAMU = 'tamu_topic'

# 💡 TIPS: Menggunakan Spreadsheet ID jauh lebih stabil daripada nama teks
# Salin ID dari URL Google Sheets kamu: https://docs.google.com/spreadsheets/d/ SPREADSHEET_ID /edit
SPREADSHEET_ID = '10DA2UJ7a-lEyCVqK2tVSZQXWhIizUKewSBJsnfX8tgA'

# Menggunakan path absolut container Airflow agar sinkron dengan proyek Anda
STATE_FILE = '/opt/airflow/scripts/producers/checkpoint_sheets.json'
CREDENTIALS_FILE = '/opt/airflow/scripts/producers/credentials.json'
# ============================================================================

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {"Undangan": 0, "Tamu": 0}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

# 2. Inisialisasi Kafka Producer
print("🔄 Menghubungkan ke Kafka Broker...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    print("✅ Kafka Producer berhasil terhubung!")
except Exception as e:
    print(f"❌ Gagal terhubung ke Kafka: {e}")
    exit(1)

# 3. Otentikasi Google Sheets API
print("🔄 Mengautentikasi ke Google API...")
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    
    # Membuka menggunakan ID agar menghindari masalah Response [200]
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    print("✅ Berhasil terhubung ke Google Sheets!")
except Exception as e:
    print(f"❌ Gagal otentikasi Google Sheets: {e}")
    print(f"👉 Pastikan file json ada di {CREDENTIALS_FILE} dan email servicenya sudah di-share ke Google Sheets!")
    exit(1)

# 4. Fungsi Sinkronisasi Data Baru
def stream_incremental(sheet_name, topic_name, current_state):
    try:
        sheet = spreadsheet.worksheet(sheet_name)
        
        # 💡 PERBAIKAN: head=3 memberi tahu gspread bahwa nama kolom/header ada di Baris 3
        # (Melewati baris judul besar di Baris 1 & baris kosong di Baris 2)
        records = sheet.get_all_records(head=3) 
        
        last_processed_row = current_state.get(sheet_name, 0)
        total_rows = len(records)
        
        if total_rows > last_processed_row:
            print(f"\n📢 [{sheet_name}] Menemukan data baru! Memproses data ke-{last_processed_row + 1} sampai {total_rows}")
            
            new_records = records[last_processed_row:total_rows]
            
            for index, record in enumerate(new_records):
                producer.send(topic_name, value=record)
                print(f"   -> Terkirim data ke-{last_processed_row + index + 1} ke Kafka")
                
            producer.flush() 
            
            current_state[sheet_name] = total_rows
            save_state(current_state)
            print(f"✨ Berhasil sinkronisasi {len(new_records)} data baru ke {topic_name}.")
        else:
            print(f"😴 [{sheet_name}] Tidak ada data baru (Posisi data terakhir: {total_rows}).")
            
    except Exception as e:
        print(f"❌ Terjadi error pada sheet {sheet_name}: {e}")

# Fungsi ini yang dipanggil oleh PythonOperator di DAG Airflow Anda
def run_producer():
    state = load_state()
    stream_incremental("Undangan", TOPIC_UNDANGAN, state)
    stream_incremental("Tamu", TOPIC_TAMU, state)

if __name__ == "__main__":
    state = load_state()
    print("\n=== Menjalankan Producer Google Sheets (Cek Data Tiap 10 Detik) ===")
    try:
        while True:
            stream_incremental("Undangan", TOPIC_UNDANGAN, state)
            stream_incremental("Tamu", TOPIC_TAMU, state)
            print("\n⏳ Menunggu 10 detik untuk pengecekan berikutnya... (Ctrl+C untuk stop)")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n👋 Script Producer dihentikan oleh user.")