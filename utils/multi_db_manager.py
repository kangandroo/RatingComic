import os
import sqlite3
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class MultipleDBManager:
    """
    Quản lý nhiều database cho các nguồn dữ liệu khác nhau
    """
    
    def __init__(self, db_folder="database"):
        """
        Khởi tạo MultipleDBManager
        
        Args:
            db_folder: Thư mục chứa database
        """
        self.db_folder = db_folder
        self.current_source = None
        
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
    
    def get_connection(self):
        """
        Lấy kết nối đến database hiện tại
        
        Returns:
            SQLite connection
        """
        if not self.current_source:
            raise ValueError("Chưa đặt nguồn dữ liệu hiện tại")
        
        db_file = os.path.join(self.db_folder, self.supported_sources[self.current_source]["file"])
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        
        # Tạo bảng nếu cần
        cursor = conn.cursor()
        for table_name, schema in self.supported_sources[self.current_source]["tables"].items():
            cursor.execute(schema)
        conn.commit()
        
        return conn
    
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
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # NOTE: Cách này không an toàn với thread, chỉ dùng trong main thread
            # Xem SQLiteHelper để triển khai thread-safe
            
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
        
        conn = self.get_connection()
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
        
        conn = self.get_connection()
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
        
        conn = self.get_connection()
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
        
        conn = self.get_connection()
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