import os
import sqlite3
import logging
import threading

logger = logging.getLogger(__name__)

class SQLiteHelper:
    """
    Helper class để thực hiện các thao tác SQLite an toàn với thread
    """
    
    def __init__(self, db_folder):
        """
        Khởi tạo SQLiteHelper
        
        Args:
            db_folder: Thư mục chứa database
        """
        self.db_folder = db_folder
        self.thread_local = threading.local()
        
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(db_folder, exist_ok=True)
        
        # Định nghĩa schema cho từng nguồn
        self.schemas = {
            "TruyenQQ": self._get_truyenqq_schema(),
            "NetTruyen": self._get_nettruyen_schema(),
            "Manhuavn": self._get_manhuavn_schema()
        }
        
        logger.info(f"Khởi tạo SQLiteHelper với db_folder: {db_folder}")
    
    def _get_truyenqq_schema(self):
        """Lấy schema cho TruyenQQ database"""
        return {
            "comics": """
                CREATE TABLE IF NOT EXISTS comics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ten_truyen TEXT NOT NULL,
                    tac_gia TEXT,
                    the_loai TEXT,
                    mo_ta TEXT,
                    link_truyen TEXT UNIQUE,
                    so_chuong INTEGER DEFAULT 0,
                    luot_xem INTEGER DEFAULT 0,
                    luot_thich INTEGER DEFAULT 0,
                    luot_theo_doi INTEGER DEFAULT 0,
                    so_binh_luan INTEGER DEFAULT 0,
                    trang_thai TEXT,
                    nguon TEXT DEFAULT 'TruyenQQ',
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "comments": """
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comic_id INTEGER,
                    ten_nguoi_binh_luan TEXT,
                    noi_dung TEXT,
                    sentiment TEXT,
                    sentiment_score REAL,
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (comic_id) REFERENCES comics (id)
                )
            """
        }
    
    def _get_nettruyen_schema(self):
        """Lấy schema cho NetTruyen database"""
        return {
            "comics": """
                CREATE TABLE IF NOT EXISTS comics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ten_truyen TEXT NOT NULL,
                    tac_gia TEXT,
                    the_loai TEXT,
                    mo_ta TEXT,
                    link_truyen TEXT UNIQUE,
                    so_chuong INTEGER DEFAULT 0,
                    luot_xem INTEGER DEFAULT 0,
                    luot_thich INTEGER DEFAULT 0,
                    luot_theo_doi INTEGER DEFAULT 0,
                    rating TEXT,
                    luot_danh_gia INTEGER DEFAULT 0,
                    so_binh_luan INTEGER DEFAULT 0,
                    trang_thai TEXT,
                    nguon TEXT DEFAULT 'NetTruyen',
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "comments": """
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comic_id INTEGER,
                    ten_nguoi_binh_luan TEXT,
                    noi_dung TEXT,
                    sentiment TEXT,
                    sentiment_score REAL,
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (comic_id) REFERENCES comics (id)
                )
            """
        }
    
    def _get_manhuavn_schema(self):
        """Lấy schema cho Manhuavn database"""
        return {
            "comics": """
                CREATE TABLE IF NOT EXISTS comics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ten_truyen TEXT NOT NULL,
                    tac_gia TEXT,
                    the_loai TEXT,
                    mo_ta TEXT,
                    link_truyen TEXT UNIQUE,
                    so_chuong INTEGER DEFAULT 0,
                    luot_xem INTEGER DEFAULT 0,
                    luot_theo_doi INTEGER DEFAULT 0,
                    danh_gia TEXT,
                    luot_danh_gia INTEGER DEFAULT 0,
                    trang_thai TEXT,
                    nguon TEXT DEFAULT 'Manhuavn',
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """,
            "comments": """
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    comic_id INTEGER,
                    ten_nguoi_binh_luan TEXT,
                    noi_dung TEXT,
                    sentiment TEXT,
                    sentiment_score REAL,
                    thoi_gian_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (comic_id) REFERENCES comics (id)
                )
            """
        }
    
    def _get_db_file(self, source_name):
        """
        Lấy đường dẫn đến file database dựa vào nguồn
        
        Args:
            source_name: Tên nguồn dữ liệu
            
        Returns:
            str: Đường dẫn đến file database
        """
        # Map nguồn dữ liệu đến tên file
        db_files = {
            "TruyenQQ": "truyenqq.db",
            "NetTruyen": "nettruyen.db",
            "Manhuavn": "manhuavn.db"
        }
        
        if source_name not in db_files:
            raise ValueError(f"Nguồn không được hỗ trợ: {source_name}")
        
        return os.path.join(self.db_folder, db_files[source_name])
    
    def _get_connection(self, source_name):
        """
        Lấy connection an toàn với thread cho nguồn dữ liệu
        
        Args:
            source_name: Tên nguồn dữ liệu
            
        Returns:
            sqlite3.Connection: SQLite connection
        """
        thread_id = threading.get_ident()
        
        # Khởi tạo dictionary connections cho thread nếu chưa có
        if not hasattr(self.thread_local, "connections"):
            self.thread_local.connections = {}
        
        # Tạo key duy nhất cho connection
        db_file = self._get_db_file(source_name)
        connection_key = f"{thread_id}_{source_name}"
        
        # Tạo connection mới nếu chưa có
        if connection_key not in self.thread_local.connections:
            try:
                # Tạo thư mục chứa database nếu chưa tồn tại
                os.makedirs(os.path.dirname(db_file), exist_ok=True)
                
                # Tạo connection
                conn = sqlite3.connect(db_file)
                conn.row_factory = sqlite3.Row
                
                # Khởi tạo schema nếu cần
                cursor = conn.cursor()
                if source_name in self.schemas:
                    for table_name, schema in self.schemas[source_name].items():
                        cursor.execute(schema)
                    conn.commit()
                
                # Lưu connection vào thread-local
                self.thread_local.connections[connection_key] = conn
                logger.info(f"Thread {thread_id}: Tạo connection mới cho {source_name}")
                
            except Exception as e:
                logger.error(f"Thread {thread_id}: Lỗi khi tạo connection cho {source_name}: {e}")
                return None
        
        return self.thread_local.connections[connection_key]
    
    def save_comic_to_db(self, comic_data, source_name):
        """
        Lưu truyện vào database an toàn với thread
        
        Args:
            comic_data: Dữ liệu truyện
            source_name: Tên nguồn
            
        Returns:
            int: ID của truyện hoặc None nếu lỗi
        """
        thread_id = threading.get_ident()
        
        # Lấy connection cho thread hiện tại
        conn = self._get_connection(source_name)
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            
            # Điều chỉnh field names tùy thuộc vào nguồn
            if source_name == "TruyenQQ":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_thich, luot_theo_doi, so_binh_luan, trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic_data.get("ten_truyen", ""),
                    comic_data.get("tac_gia", "N/A"),
                    comic_data.get("the_loai", ""),
                    comic_data.get("mo_ta", ""),
                    comic_data.get("link_truyen", ""),
                    comic_data.get("so_chuong", 0),
                    comic_data.get("luot_xem", 0),
                    comic_data.get("luot_thich", 0),
                    comic_data.get("luot_theo_doi", 0),
                    comic_data.get("so_binh_luan", 0),
                    comic_data.get("trang_thai", ""),
                    comic_data.get("nguon", "TruyenQQ")
                )
            elif source_name == "NetTruyen":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_thich, luot_theo_doi, rating, luot_danh_gia, 
                     so_binh_luan, trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic_data.get("ten_truyen", ""),
                    comic_data.get("tac_gia", "N/A"),
                    comic_data.get("the_loai", ""),
                    comic_data.get("mo_ta", ""),
                    comic_data.get("link_truyen", ""),
                    comic_data.get("so_chuong", 0),
                    comic_data.get("luot_xem", 0),
                    comic_data.get("luot_thich", 0),
                    comic_data.get("luot_theo_doi", 0),
                    comic_data.get("rating", ""),
                    comic_data.get("luot_danh_gia", 0),
                    comic_data.get("so_binh_luan", 0),
                    comic_data.get("trang_thai", ""),
                    comic_data.get("nguon", "NetTruyen")
                )
            elif source_name == "Manhuavn":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_theo_doi, danh_gia, luot_danh_gia, 
                     trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic_data.get("ten_truyen", ""),
                    comic_data.get("tac_gia", "N/A"),
                    comic_data.get("the_loai", ""),
                    comic_data.get("mo_ta", ""),
                    comic_data.get("link_truyen", ""),
                    comic_data.get("so_chuong", 0),
                    comic_data.get("luot_xem", 0),
                    comic_data.get("luot_theo_doi", 0),
                    comic_data.get("danh_gia", ""),
                    comic_data.get("luot_danh_gia", 0),
                    comic_data.get("trang_thai", ""),
                    comic_data.get("nguon", "Manhuavn")
                )
            else:
                logger.error(f"Thread {thread_id}: Nguồn không được hỗ trợ: {source_name}")
                return None
            
            cursor.execute(query, params)
            conn.commit()
            
            # Lấy ID của truyện vừa thêm/cập nhật
            cursor.execute("SELECT id FROM comics WHERE link_truyen = ?", (comic_data.get("link_truyen", ""),))
            result = cursor.fetchone()
            
            return result["id"] if result else None
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Lỗi khi lưu truyện vào database: {e}")
            return None
    
    def save_comments_to_db(self, comic_id, comments_data, source_name):
        """
        Lưu bình luận vào database an toàn với thread
        
        Args:
            comic_id: ID của truyện
            comments_data: Danh sách bình luận
            source_name: Tên nguồn
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if not comments_data:
            return True
            
        thread_id = threading.get_ident()
        
        # Lấy connection cho thread hiện tại
        conn = self._get_connection(source_name)
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            
            # Xóa các bình luận cũ của truyện này
            cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
            
            # Thêm các bình luận mới
            for comment in comments_data:
                cursor.execute('''
                    INSERT INTO comments 
                    (comic_id, ten_nguoi_binh_luan, noi_dung, sentiment, sentiment_score)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    comic_id,
                    comment.get("ten_nguoi_binh_luan", ""),
                    comment.get("noi_dung", ""),
                    comment.get("sentiment", ""),
                    comment.get("sentiment_score", 0)
                ))
            
            conn.commit()
            logger.info(f"Thread {thread_id}: Đã lưu {len(comments_data)} bình luận cho truyện ID {comic_id}")
            return True
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Lỗi khi lưu bình luận: {e}")
            return False
    
    def get_all_comics(self, source_name):
        """
        Lấy tất cả truyện từ một nguồn
        
        Args:
            source_name: Tên nguồn dữ liệu
            
        Returns:
            list: Danh sách truyện
        """
        thread_id = threading.get_ident()
        
        # Lấy connection cho thread hiện tại
        conn = self._get_connection(source_name)
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comics")
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Lỗi khi lấy danh sách truyện: {e}")
            return []
    
    def get_comic_by_id(self, comic_id, source_name):
        """
        Lấy thông tin truyện theo ID
        
        Args:
            comic_id: ID của truyện
            source_name: Tên nguồn dữ liệu
            
        Returns:
            dict: Thông tin truyện
        """
        thread_id = threading.get_ident()
        
        # Lấy connection cho thread hiện tại
        conn = self._get_connection(source_name)
        if not conn:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comics WHERE id = ?", (comic_id,))
            row = cursor.fetchone()
            
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Lỗi khi lấy thông tin truyện: {e}")
            return None
    
    def get_comments_by_comic_id(self, comic_id, source_name):
        """
        Lấy bình luận của truyện theo ID
        
        Args:
            comic_id: ID của truyện
            source_name: Tên nguồn dữ liệu
            
        Returns:
            list: Danh sách bình luận
        """
        thread_id = threading.get_ident()
        
        # Lấy connection cho thread hiện tại
        conn = self._get_connection(source_name)
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comments WHERE comic_id = ?", (comic_id,))
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Thread {thread_id}: Lỗi khi lấy bình luận: {e}")
            return []
    
    def close_all_connections(self):
        """Đóng tất cả kết nối của thread hiện tại"""
        thread_id = threading.get_ident()
        
        if hasattr(self.thread_local, "connections"):
            connection_count = len(self.thread_local.connections)
            for conn in self.thread_local.connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.debug(f"Thread {thread_id}: Lỗi khi đóng connection: {e}")
            
            self.thread_local.connections = {}
            logger.info(f"Thread {thread_id}: Đã đóng {connection_count} kết nối")