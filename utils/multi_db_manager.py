import os
import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class MultipleDBManager:
    """
    Quản lý nhiều database cho các nguồn dữ liệu khác nhau
    """
    
    def __init__(self, db_folder="database", pool_size=5):
        """
        Khởi tạo MultipleDBManager
        
        Args:
            db_folder: Thư mục chứa database
        """
        self.db_folder = db_folder
        self.current_source = None
        self.pool_size = pool_size
        self.connection_pools = {}  # Pool kết nối SQLite theo nguồn
        
        # Tạo thư mục database nếu chưa tồn tại
        os.makedirs(db_folder, exist_ok=True)
        
        # Định nghĩa các nguồn dữ liệu được hỗ trợ
        self.supported_sources = {
            "TruyenQQ": {
                "file": "truyenqq.db",
                "tables": self.get_truyenqq_schema()
            },
            "NetTruyen": {
                "file": "nettruyen.db",
                "tables": self.get_nettruyen_schema()
            },
            "Manhuavn": {
                "file": "manhuavn.db",
                "tables": self.get_manhuavn_schema()
            }
        }
        
        logger.info(f"Khởi tạo MultipleDBManager với thư mục: {db_folder}")
    
    def get_truyenqq_schema(self):
        """Schema cho TruyenQQ database"""
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
                    base_rating REAL DEFAULT NULL,
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
    
    def get_nettruyen_schema(self):
        """Schema cho NetTruyen database"""
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
                    base_rating REAL DEFAULT NULL,
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
    
    def get_manhuavn_schema(self):
        """Schema cho Manhuavn database"""
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
                    base_rating REAL DEFAULT NULL,
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
    
    def set_source(self, source):
        """
        Thiết lập nguồn dữ liệu hiện tại
        
        Args:
            source: Tên nguồn dữ liệu
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if source not in self.supported_sources:
            logger.error(f"Nguồn dữ liệu không được hỗ trợ: {source}")
            return False
        
        self.current_source = source
        logger.info(f"Đã thiết lập nguồn dữ liệu: {source}")
        return True
    
    def save_base_rating(self, comic_id, base_rating):
        """Lưu điểm cơ bản vào database"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Cập nhật trường base_rating
            cursor.execute(
                "UPDATE comics SET base_rating = ? WHERE id = ?",
                (base_rating, comic_id)
            )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu base_rating: {e}")
            return False

    def save_batch_ratings(self, ratings_data):
        """Lưu nhiều rating cùng lúc"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            for comic_id, rating in ratings_data.items():
                cursor.execute(
                    "UPDATE comics SET base_rating = ? WHERE id = ?",
                    (rating, comic_id)
                )
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu batch ratings: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False    
    
    def update_comics_rating(self, comics):
        """Cập nhật rating của nhiều truyện cùng lúc"""
        if not comics:
            return False
            
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            for comic in comics:
                cursor.execute(
                    "UPDATE comics SET base_rating = ? WHERE id = ?",
                    (comic.get("base_rating"), comic.get("id"))
                )
            
            conn.commit()
            # logger.info(f"Đã cập nhật rating cho {len(comics)} truyện")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật rating: {e}")
            conn.rollback()
            return False
            
        finally:
            conn.close()
    
    def _initialize_pool(self, source):
        """Khởi tạo connection pool cho nguồn dữ liệu"""
        if source not in self.connection_pools:
            self.connection_pools[source] = []
            db_file = os.path.join(self.db_folder, self.supported_sources[source]["file"])
            
            # Tạo các kết nối cho pool
            for _ in range(self.pool_size):
                conn = sqlite3.connect(db_file, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                
                # Tạo bảng nếu cần
                cursor = conn.cursor()
                for table_name, schema in self.supported_sources[source]["tables"].items():
                    cursor.execute(schema)
                conn.commit()
                
                self.connection_pools[source].append(conn)
            
            logger.info(f"Đã khởi tạo connection pool cho nguồn: {source}")

    def _get_connection_from_pool(self):
        """Lấy kết nối từ pool, tạo mới nếu pool rỗng"""
        if not self.current_source:
            raise ValueError("Chưa đặt nguồn dữ liệu hiện tại")
        
        # Khởi tạo pool nếu chưa có
        if self.current_source not in self.connection_pools:
            self._initialize_pool(self.current_source)
        
        # Lấy connection từ pool
        if self.connection_pools[self.current_source]:
            return self.connection_pools[self.current_source].pop()
        else:
            # Nếu hết connection, tạo mới
            db_file = os.path.join(self.db_folder, self.supported_sources[self.current_source]["file"])
            conn = sqlite3.connect(db_file, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn

    def _return_connection_to_pool(self, conn):
        """Trả kết nối về pool"""
        if not self.current_source:
            conn.close()
            return
        
        if self.current_source in self.connection_pools:
            # Chỉ giữ số lượng connection giới hạn trong pool
            if len(self.connection_pools[self.current_source]) < self.pool_size:
                self.connection_pools[self.current_source].append(conn)
            else:
                conn.close()
        else:
            conn.close()
    
    def _get_connection(self):
        """
        Lấy kết nối đến database hiện tại
        
        Returns:
            SQLite connection
        """
        if not self.current_source:
            raise ValueError("Chưa đặt nguồn dữ liệu hiện tại")
        
        # Sử dụng pool nếu có
        if hasattr(self, 'connection_pools') and self.current_source in self.connection_pools:
            return self._get_connection_from_pool()
        
        # Fallback to old method
        db_file = os.path.join(self.db_folder, self.supported_sources[self.current_source]["file"])
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        
        # Tạo bảng nếu cần
        cursor = conn.cursor()
        for table_name, schema in self.supported_sources[self.current_source]["tables"].items():
            cursor.execute(schema)
        conn.commit()
        
        return conn
    
    def save_comics_batch(self, comics_list):
        """
        Lưu nhiều truyện vào database trong một transaction
        
        Args:
            comics_list: List dictionary chứa dữ liệu nhiều truyện
                
        Returns:
            list: List các ID của truyện đã lưu
        """
        if not self.current_source or not comics_list:
            return []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        comic_ids = []
        
        try:
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            for comic in comics_list:
                # Xác định nguồn dữ liệu hiện tại - code tương tự như save_comic()
                if self.current_source == "TruyenQQ":
                    query = """
                        INSERT OR REPLACE INTO comics 
                        (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                        luot_xem, luot_thich, luot_theo_doi, so_binh_luan, trang_thai, nguon)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        comic.get("ten_truyen", ""),
                        comic.get("tac_gia", "N/A"),
                        comic.get("the_loai", ""),
                        comic.get("mo_ta", ""),
                        comic.get("link_truyen", ""),
                        comic.get("so_chuong", 0),
                        comic.get("luot_xem", 0),
                        comic.get("luot_thich", 0),
                        comic.get("luot_theo_doi", 0),
                        comic.get("so_binh_luan", 0),
                        comic.get("trang_thai", ""),
                        comic.get("nguon", "TruyenQQ")
                    )
                elif self.current_source == "NetTruyen":
                    query = """
                        INSERT OR REPLACE INTO comics 
                        (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                        luot_xem, luot_thich, luot_theo_doi, rating, luot_danh_gia, 
                        so_binh_luan, trang_thai, nguon)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        comic.get("ten_truyen", ""),
                        comic.get("tac_gia", "N/A"),
                        comic.get("the_loai", ""),
                        comic.get("mo_ta", ""),
                        comic.get("link_truyen", ""),
                        comic.get("so_chuong", 0),
                        comic.get("luot_xem", 0),
                        comic.get("luot_thich", 0),
                        comic.get("luot_theo_doi", 0),
                        comic.get("rating", ""),
                        comic.get("luot_danh_gia", 0),
                        comic.get("so_binh_luan", 0),
                        comic.get("trang_thai", ""),
                        comic.get("nguon", "NetTruyen")
                    )
                elif self.current_source == "Manhuavn":
                    query = """
                        INSERT OR REPLACE INTO comics 
                        (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                        luot_xem, luot_theo_doi, danh_gia, luot_danh_gia, 
                        trang_thai, nguon)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    params = (
                        comic.get("ten_truyen", ""),
                        comic.get("tac_gia", "N/A"),
                        comic.get("the_loai", ""),
                        comic.get("mo_ta", ""),
                        comic.get("link_truyen", ""),
                        comic.get("so_chuong", 0),
                        comic.get("luot_xem", 0),
                        comic.get("luot_theo_doi", 0),
                        comic.get("danh_gia", ""),
                        comic.get("luot_danh_gia", 0),
                        comic.get("trang_thai", ""),
                        comic.get("nguon", "Manhuavn")
                    )
                    
                cursor.execute(query, params)
                
                # Lấy ID của truyện vừa thêm/cập nhật
                cursor.execute("SELECT id FROM comics WHERE link_truyen = ?", (comic.get("link_truyen", ""),))
                result = cursor.fetchone()
                
                if result:
                    comic_ids.append(result["id"])
            
            # Commit transaction chỉ một lần cho tất cả records
            conn.commit()
            logger.info(f"Đã lưu batch {len(comics_list)} truyện vào DB")
            return comic_ids
                
        except Exception as e:
            logger.error(f"Lỗi khi lưu batch truyện: {str(e)}")
            conn.rollback()
            return []
        finally:
            conn.close()
            
    def save_comments_batch(self, comments_batch):
        """
        Lưu nhiều bình luận từ nhiều truyện vào database trong một transaction
        
        Args:
            comments_batch: Dictionary với key là comic_id và value là list comments
        """
        if not self.current_source or not comments_batch:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            total_comments = 0
            
            for comic_id, comments in comments_batch.items():
                if not comments:
                    continue
                    
                # Xóa comments cũ
                cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
                
                # Chuẩn bị dữ liệu cho executemany
                comment_params = [
                    (
                        comic_id,
                        comment.get("ten_nguoi_binh_luan", ""),
                        comment.get("noi_dung", ""),
                        comment.get("sentiment", ""),
                        comment.get("sentiment_score", 0)
                    )
                    for comment in comments
                ]
                
                # Thêm comments mới với executemany (nhanh hơn execute nhiều lần)
                cursor.executemany('''
                    INSERT INTO comments 
                    (comic_id, ten_nguoi_binh_luan, noi_dung, sentiment, sentiment_score)
                    VALUES (?, ?, ?, ?, ?)
                ''', comment_params)
                
                total_comments += len(comments)
            
            # Commit transaction
            conn.commit()
            logger.info(f"Đã lưu tổng cộng {total_comments} bình luận cho {len(comments_batch)} truyện")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu batch bình luận: {str(e)}")
            conn.rollback()
        finally:
            conn.close()        
    
    def save_comic(self, comic):
        """
        Lưu truyện vào database
        
        Args:
            comic: Dictionary chứa dữ liệu truyện
            
        Returns:
            int: ID của truyện
        """
        if not self.current_source:
            raise ValueError("Chưa đặt nguồn dữ liệu hiện tại")
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Điều chỉnh field names tùy thuộc vào nguồn
            if self.current_source == "TruyenQQ":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_thich, luot_theo_doi, so_binh_luan, trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic.get("ten_truyen", ""),
                    comic.get("tac_gia", "N/A"),
                    comic.get("the_loai", ""),
                    comic.get("mo_ta", ""),
                    comic.get("link_truyen", ""),
                    comic.get("so_chuong", 0),
                    comic.get("luot_xem", 0),
                    comic.get("luot_thich", 0),
                    comic.get("luot_theo_doi", 0),
                    comic.get("so_binh_luan", 0),
                    comic.get("trang_thai", ""),
                    comic.get("nguon", "TruyenQQ")
                )
            elif self.current_source == "NetTruyen":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_thich, luot_theo_doi, rating, luot_danh_gia, 
                     so_binh_luan, trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic.get("ten_truyen", ""),
                    comic.get("tac_gia", "N/A"),
                    comic.get("the_loai", ""),
                    comic.get("mo_ta", ""),
                    comic.get("link_truyen", ""),
                    comic.get("so_chuong", 0),
                    comic.get("luot_xem", 0),
                    comic.get("luot_thich", 0),
                    comic.get("luot_theo_doi", 0),
                    comic.get("rating", ""),
                    comic.get("luot_danh_gia", 0),
                    comic.get("so_binh_luan", 0),
                    comic.get("trang_thai", ""),
                    comic.get("nguon", "NetTruyen")
                )
            elif self.current_source == "Manhuavn":
                query = """
                    INSERT OR REPLACE INTO comics 
                    (ten_truyen, tac_gia, the_loai, mo_ta, link_truyen, so_chuong, 
                     luot_xem, luot_theo_doi, danh_gia, luot_danh_gia, 
                     trang_thai, nguon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = (
                    comic.get("ten_truyen", ""),
                    comic.get("tac_gia", "N/A"),
                    comic.get("the_loai", ""),
                    comic.get("mo_ta", ""),
                    comic.get("link_truyen", ""),
                    comic.get("so_chuong", 0),
                    comic.get("luot_xem", 0),
                    comic.get("luot_theo_doi", 0),
                    comic.get("danh_gia", ""),
                    comic.get("luot_danh_gia", 0),
                    comic.get("trang_thai", ""),
                    comic.get("nguon", "Manhuavn")
                )
                
            cursor.execute(query, params)
            conn.commit()
            
            # Lấy ID của truyện vừa thêm/cập nhật
            cursor.execute("SELECT id FROM comics WHERE link_truyen = ?", (comic.get("link_truyen", ""),))
            result = cursor.fetchone()
            
            return result["id"] if result else None
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu truyện vào database: {str(e)}")
            return None
        finally:
            conn.close()
    
    def save_comments(self, comic_id, comments):
        """
        Lưu bình luận vào database
        
        Args:
            comic_id: ID của truyện
            comments: List các bình luận
        """
        if not self.current_source or not comments:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Xóa comments cũ
            cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
            
            # Thêm comments mới
            for comment in comments:
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
            logger.info(f"Đã lưu {len(comments)} bình luận cho truyện ID {comic_id}")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu bình luận: {str(e)}")
        finally:
            conn.close()
    
    def get_all_comics(self, source=None):
        """
        Lấy tất cả truyện từ database
        
        Args:
            source: Nguồn dữ liệu (nếu None, sử dụng nguồn hiện tại)
            
        Returns:
            list: Danh sách truyện
        """
        old_source = self.current_source
        if source:
            self.set_source(source)
        
        if not self.current_source:
            return []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM comics")
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            result = [dict(row) for row in rows]
            
            return result
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {str(e)}")
            return []
        finally:
            conn.close()
            if source and old_source:
                self.set_source(old_source)
    
    def get_comic_by_id(self, comic_id):
        """
        Lấy thông tin truyện theo ID
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            dict: Thông tin truyện
        """
        if not self.current_source:
            return None
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM comics WHERE id = ?", (comic_id,))
            row = cursor.fetchone()
            
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin truyện ID {comic_id}: {str(e)}")
            return None
        finally:
            conn.close()
    
    def get_all_comments(self, comic_id):
        """
        Lấy tất cả bình luận của truyện
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            list: Danh sách bình luận
        """
        if not self.current_source:
            return []
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM comments WHERE comic_id = ?", (comic_id,))
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            comments = [dict(row) for row in rows]
            
            return comments
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy bình luận cho truyện ID {comic_id}: {str(e)}")
            return []
        finally:
            conn.close()
    
    def export_results_to_excel(self, results, output_file):
        """
        Xuất kết quả phân tích ra file Excel
        
        Args:
            results: Kết quả phân tích
            output_file: Đường dẫn file output
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            # Chuyển kết quả thành DataFrame
            comics_data = []
            comments_data = []
            
            for result in results:
                # Dữ liệu truyện
                comic_data = {
                    "ID": result.get("id", ""),
                    "Tên truyện": result.get("ten_truyen", ""),
                    "Tác giả": result.get("tac_gia", ""),
                    "Thể loại": result.get("the_loai", ""),
                    "Số chương": result.get("so_chuong", 0),
                    "Lượt xem": result.get("luot_xem", 0),
                    "Lượt theo dõi": result.get("luot_theo_doi", 0),
                    "Trạng thái": result.get("trang_thai", ""),
                    "Nguồn": result.get("nguon", ""),
                    "Điểm cơ bản": result.get("base_rating", 0),
                    "Điểm sentiment": result.get("sentiment_rating", 0),
                    "Điểm tổng hợp": result.get("comprehensive_rating", 0),
                    "Số comment": len(result.get("comments", []))
                }
                
                # Thêm thông tin riêng theo nguồn
                if result.get("nguon") == "NetTruyen":
                    comic_data["Rating"] = result.get("rating", "")
                    comic_data["Lượt đánh giá"] = result.get("luot_danh_gia", 0)
                elif result.get("nguon") == "TruyenQQ":
                    comic_data["Lượt thích"] = result.get("luot_thich", 0)
                elif result.get("nguon") == "Manhuavn":
                    comic_data["Đánh giá"] = result.get("danh_gia", "")
                    comic_data["Lượt đánh giá"] = result.get("luot_danh_gia", 0)
                
                comics_data.append(comic_data)
                
                # Dữ liệu comment
                for comment in result.get("comments", []):
                    comment_data = {
                        "Tên truyện": result.get("ten_truyen", ""),
                        "Người bình luận": comment.get("ten_nguoi_binh_luan", ""),
                        "Nội dung": comment.get("noi_dung", ""),
                        "Sentiment": comment.get("sentiment", ""),
                        "Điểm sentiment": comment.get("sentiment_score", 0)
                    }
                    comments_data.append(comment_data)
            
            # Tạo Excel Writer
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Sheet truyện
                comics_df = pd.DataFrame(comics_data)
                comics_df.to_excel(writer, sheet_name='Danh sách truyện', index=False)
                
                # Sheet comment
                comments_df = pd.DataFrame(comments_data)
                comments_df.to_excel(writer, sheet_name='Bình luận', index=False)
            
            logger.info(f"Đã xuất kết quả ra file: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất kết quả ra file Excel: {str(e)}")
            return False
        
    def delete_sentiment_analysis(self, comic_id):
        """
        Xóa phân tích sentiment của một truyện
        """
        try:
            # Lấy kết nối từ nguồn dữ liệu hiện tại
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Cập nhật bảng comments để đặt các cột sentiment về NULL
            cursor.execute("""
                UPDATE comments 
                SET sentiment = NULL, sentiment_score = NULL 
                WHERE comic_id = ?
            """, (comic_id,))
            
            # Commit thay đổi
            conn.commit()
            
            logger.info(f"Đã xóa phân tích sentiment cho comic ID: {comic_id}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xóa phân tích sentiment: {str(e)}")
            if 'conn' in locals():
                conn.rollback()
            return False
        finally:
            if 'conn' in locals():
                conn.close()