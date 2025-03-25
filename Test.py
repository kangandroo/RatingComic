import sqlite3

def fetch_db_structure(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("📌 Danh sách các bảng trong database:")
    for table in tables:
        table_name = table[0]
        print(f"\n🔹 Bảng: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        print("────────────────────────────────────────────────────────────")
        print("ID | Tên cột | Kiểu dữ liệu | NOT NULL | Default | Primary Key")
        print("────────────────────────────────────────────────────────────")
        for col in columns:
            print(" | ".join(str(c) for c in col))
    
    conn.close()


db_path = r"C:\Users\Hi\rating_comic\code\RatingComic\database\nettruyen.db"  # Thay bằng đường dẫn thực tế của bạn
fetch_db_structure(db_path)
