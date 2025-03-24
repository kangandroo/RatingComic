import sqlite3

def fetch_comments(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Lấy tất cả dữ liệu từ bảng comments
    cursor.execute("SELECT * FROM comments;")
    rows = cursor.fetchall()

    # Lấy danh sách tên cột
    columns = [desc[0] for desc in cursor.description]

    print("\n📌 Dữ liệu trong bảng comments:")
    print("────────────────────────────────────────────────────────────")
    print(" | ".join(columns))  # In tên cột
    print("────────────────────────────────────────────────────────────")
    
    for row in rows[:10]:  # Giới hạn 10 dòng đầu
        print(" | ".join(str(cell) for cell in row))
    
    conn.close()

# Chạy hàm với đường dẫn file SQLite
db_path = r"C:\Users\Hi\rating_comic\code\RatingComic\database\truyenqq.db"  # Thay bằng đường dẫn thực tế của bạn
fetch_comments(db_path)

