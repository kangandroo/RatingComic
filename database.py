import os
import sqlite3
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đường dẫn đến database
DB_FILE = 'data/truyenqq_data.db'

def init_database():
    """Khởi tạo cơ sở dữ liệu SQLite và tạo các bảng cần thiết"""
    # Đảm bảo thư mục data tồn tại
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    
    # Kiểm tra và xóa file cũ nếu tồn tại để tránh trùng lặp dữ liệu
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        logger.info(f"Đã xóa database cũ: {DB_FILE}")
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Tạo bảng lưu thông tin truyện
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ten_truyen TEXT NOT NULL,
        link_truyen TEXT NOT NULL UNIQUE,
        ten_khac TEXT,
        tac_gia TEXT,
        trang_thai TEXT,
        luot_thich TEXT,
        luot_theo_doi TEXT,
        luot_xem TEXT,
        mo_ta TEXT,
        so_chuong INTEGER DEFAULT 0,
        so_binh_luan INTEGER DEFAULT 0
    )
    ''')
    
    # Tạo bảng lưu thông tin bình luận
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        story_id INTEGER,
        ten_nguoi_binh_luan TEXT,
        noi_dung_binh_luan TEXT,
        sentiment TEXT,
        sentiment_score REAL,
        FOREIGN KEY (story_id) REFERENCES stories (id)
    )
    ''')
    
    # Commit thay đổi và đóng kết nối
    conn.commit()
    conn.close()
    logger.info(f"Đã tạo database mới: {DB_FILE}")

def get_db_connection():
    """Tạo và trả về kết nối đến database"""
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row  # Kết quả truy vấn sẽ là dictionary-like objects
    return conn

def save_story(story_data):
    """Lưu thông tin truyện vào database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT OR REPLACE INTO stories (
            ten_truyen, link_truyen, ten_khac, tac_gia, trang_thai, 
            luot_thich, luot_theo_doi, luot_xem, mo_ta, so_chuong
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            story_data.get('ten_truyen', ''),
            story_data.get('link_truyen', ''),
            story_data.get('ten_khac', ''),
            story_data.get('tac_gia', ''),
            story_data.get('trang_thai', ''),
            story_data.get('luot_thich', ''),
            story_data.get('luot_theo_doi', ''),
            story_data.get('luot_xem', ''),
            story_data.get('mo_ta', ''),
            story_data.get('so_chuong', 0)
        ))
        
        story_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return story_id
        
    except Exception as e:
        logger.error(f"Lỗi khi lưu truyện: {e}")
        if conn:
            conn.close()
        return None

def save_comments(story_id, comments):
    """Lưu danh sách bình luận vào database"""
    try:
        if not comments:
            return 0
            
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Đếm số bình luận đã lưu
        count = 0
        
        # Lưu từng bình luận
        for comment in comments:
            cursor.execute('''
            INSERT INTO comments (story_id, ten_nguoi_binh_luan, noi_dung_binh_luan)
            VALUES (?, ?, ?)
            ''', (
                story_id,
                comment.get('ten_nguoi_binh_luan', ''),
                comment.get('noi_dung_binh_luan', '')
            ))
            count += 1
        
        # Cập nhật số bình luận trong bảng stories
        cursor.execute('''
        UPDATE stories SET so_binh_luan = ? WHERE id = ?
        ''', (count, story_id))
        
        conn.commit()
        conn.close()
        return count
        
    except Exception as e:
        logger.error(f"Lỗi khi lưu bình luận: {e}")
        if conn:
            conn.close()
        return 0

def get_all_stories():
    """Lấy danh sách tất cả truyện từ database"""
    try:
        conn = get_db_connection()
        stories = conn.execute('SELECT * FROM stories').fetchall()
        conn.close()
        return stories
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
        return []

def get_story_by_id(story_id):
    """Lấy thông tin truyện theo ID"""
    try:
        conn = get_db_connection()
        story = conn.execute('SELECT * FROM stories WHERE id = ?', (story_id,)).fetchone()
        conn.close()
        return story
    except Exception as e:
        logger.error(f"Lỗi khi lấy thông tin truyện ID={story_id}: {e}")
        return None

def get_comments_by_story_id(story_id):
    """Lấy danh sách bình luận của một truyện"""
    try:
        conn = get_db_connection()
        comments = conn.execute('''
            SELECT * FROM comments 
            WHERE story_id = ? 
            ORDER BY id ASC
        ''', (story_id,)).fetchall()
        conn.close()
        return comments
    except Exception as e:
        logger.error(f"Lỗi khi lấy bình luận của truyện ID={story_id}: {e}")
        return []

def update_comment_sentiment(comment_id, sentiment, score):
    """Cập nhật kết quả phân tích cảm xúc cho một bình luận"""
    try:
        conn = get_db_connection()
        conn.execute('''
            UPDATE comments 
            SET sentiment = ?, sentiment_score = ? 
            WHERE id = ?
        ''', (sentiment, score, comment_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Lỗi khi cập nhật sentiment cho bình luận ID={comment_id}: {e}")
        return False