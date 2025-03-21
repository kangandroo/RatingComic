import sqlite3
import logging
import os
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Quản lý cơ sở dữ liệu SQLite"""
    
    def __init__(self, db_file):
        """
        Khởi tạo DatabaseManager
        
        Args:
            db_file: Đường dẫn đến file SQLite
        """
        self.db_file = db_file
    
    def get_connection(self):
        """Lấy kết nối đến database"""
        return sqlite3.connect(self.db_file)
    
    def setup_database(self):
        """Thiết lập cấu trúc database ban đầu"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Tạo bảng comics
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS comics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ten_truyen TEXT NOT NULL,
                ten_khac TEXT,
                tac_gia TEXT,
                trang_thai TEXT,
                the_loai TEXT,
                luot_xem INTEGER DEFAULT 0,
                luot_thich INTEGER DEFAULT 0,
                luot_theo_doi INTEGER DEFAULT 0,
                link_truyen TEXT UNIQUE,
                mo_ta TEXT,
                so_chuong INTEGER DEFAULT 0,
                so_binh_luan INTEGER DEFAULT 0,
                nguon TEXT,
                ngay_cap_nhat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            # Tạo bảng comments
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                comic_id INTEGER,
                ten_nguoi_binh_luan TEXT,
                noi_dung TEXT,
                sentiment TEXT,
                sentiment_score REAL DEFAULT 0.5,
                FOREIGN KEY (comic_id) REFERENCES comics (id)
            )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info("Đã thiết lập cấu trúc database")
            
        except Exception as e:
            logger.error(f"Lỗi khi thiết lập database: {str(e)}")
    
    def clear_comics_data(self):
        """Xóa tất cả dữ liệu trong database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("DELETE FROM comments")
            cursor.execute("DELETE FROM comics")
            
            conn.commit()
            conn.close()
            
            logger.info("Đã xóa tất cả dữ liệu cũ")
            
        except Exception as e:
            logger.error(f"Lỗi khi xóa dữ liệu: {str(e)}")
    
    def save_comic(self, comic_data):
        """
        Lưu thông tin truyện vào database
        
        Args:
            comic_data: Dict chứa thông tin truyện
            
        Returns:
            ID của truyện trong database
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Kiểm tra xem đã tồn tại chưa
            cursor.execute(
                "SELECT id FROM comics WHERE link_truyen = ?", 
                (comic_data.get("link_truyen"),)
            )
            
            result = cursor.fetchone()
            
            if result:
                # Cập nhật nếu đã tồn tại
                comic_id = result[0]
                
                cursor.execute('''
                UPDATE comics SET
                    ten_truyen = ?,
                    ten_khac = ?,
                    tac_gia = ?,
                    trang_thai = ?,
                    the_loai = ?,
                    luot_xem = ?,
                    luot_thich = ?,
                    luot_theo_doi = ?,
                    mo_ta = ?,
                    so_chuong = ?,
                    so_binh_luan = ?,
                    ngay_cap_nhat = CURRENT_TIMESTAMP
                WHERE id = ?
                ''', (
                    comic_data.get("ten_truyen"),
                    comic_data.get("ten_khac"),
                    comic_data.get("tac_gia"),
                    comic_data.get("trang_thai"),
                    comic_data.get("the_loai"),
                    comic_data.get("luot_xem"),
                    comic_data.get("luot_thich"),
                    comic_data.get("luot_theo_doi"),
                    comic_data.get("mo_ta"),
                    comic_data.get("so_chuong"),
                    comic_data.get("so_binh_luan"),
                    comic_id
                ))
                
                logger.debug(f"Đã cập nhật truyện: {comic_data.get('ten_truyen')}")
                
            else:
                # Thêm mới nếu chưa tồn tại
                cursor.execute('''
                INSERT INTO comics (
                    ten_truyen, ten_khac, tac_gia, trang_thai, the_loai,
                    luot_xem, luot_thich, luot_theo_doi, link_truyen,
                    mo_ta, so_chuong, so_binh_luan, nguon
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    comic_data.get("ten_truyen"),
                    comic_data.get("ten_khac"),
                    comic_data.get("tac_gia"),
                    comic_data.get("trang_thai"),
                    comic_data.get("the_loai"),
                    comic_data.get("luot_xem"),
                    comic_data.get("luot_thich"),
                    comic_data.get("luot_theo_doi"),
                    comic_data.get("link_truyen"),
                    comic_data.get("mo_ta"),
                    comic_data.get("so_chuong"),
                    comic_data.get("so_binh_luan"),
                    comic_data.get("nguon")
                ))
                
                comic_id = cursor.lastrowid
                logger.debug(f"Đã thêm truyện mới: {comic_data.get('ten_truyen')}")
            
            conn.commit()
            conn.close()
            
            return comic_id
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu truyện: {str(e)}")
            return None
    
    def save_comments(self, comic_id, comments):
        """
        Lưu comment vào database
        
        Args:
            comic_id: ID của truyện trong database
            comments: List các comment
        """
        if not comments:
            return
        
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Xóa comment cũ
            cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
            
            # Thêm comment mới
            for comment in comments:
                cursor.execute('''
                INSERT INTO comments (
                    comic_id, ten_nguoi_binh_luan, noi_dung, sentiment, sentiment_score
                ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    comic_id,
                    comment.get("ten_nguoi_binh_luan"),
                    comment.get("noi_dung"),
                    comment.get("sentiment", "neutral"),
                    comment.get("sentiment_score", 0.5)
                ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Đã lưu {len(comments)} comment cho truyện ID: {comic_id}")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu comment: {str(e)}")
    
    def get_all_comics(self):
        """
        Lấy tất cả truyện từ database
        
        Returns:
            List các truyện
        """
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT * FROM comics ORDER BY ten_truyen
            """)
            
            rows = cursor.fetchall()
            
            # Chuyển từ Row object sang dict
            comics = [dict(row) for row in rows]
            
            conn.close()
            
            return comics
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {str(e)}")
            return []
    
    def get_all_genres(self):
        """
        Lấy tất cả thể loại từ database
        
        Returns:
            List các thể loại
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT DISTINCT the_loai FROM comics
            """)
            
            rows = cursor.fetchall()
            
            # Tách thể loại ra từ chuỗi
            genres = set()
            for row in rows:
                if row[0]:
                    genre_list = [g.strip() for g in row[0].split(",")]
                    genres.update(genre_list)
            
            conn.close()
            
            # Sắp xếp và loại bỏ giá trị 'N/A'
            return sorted([g for g in genres if g and g != "N/A"])
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách thể loại: {str(e)}")
            return []
    
    def get_comic_comments(self, comic_id):
        """
        Lấy tất cả comment của một truyện
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            List các comment
        """
        try:
            conn = self.get_connection()
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
            SELECT * FROM comments WHERE comic_id = ? ORDER BY id
            """, (comic_id,))
            
            rows = cursor.fetchall()
            
            # Chuyển từ Row object sang dict
            comments = [dict(row) for row in rows]
            
            conn.close()
            
            return comments
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy comment của truyện ID {comic_id}: {str(e)}")
            return []
    
    def export_results_to_excel(self, analysis_results, filename):
        """
        Xuất kết quả phân tích ra file Excel
        
        Args:
            analysis_results: Kết quả phân tích
            filename: Tên file xuất
        """
        try:
            # Tạo DataFrame cho truyện
            comics_data = []
            
            for i, comic in enumerate(analysis_results):
                comics_data.append({
                    "Xếp hạng": i + 1,
                    "Tên truyện": comic["ten_truyen"],
                    "Tên khác": comic["ten_khac"],
                    "Tác giả": comic["tac_gia"],
                    "Thể loại": comic["the_loai"],
                    "Trạng thái": comic["trang_thai"],
                    "Số chương": comic["so_chuong"],
                    "Lượt xem": comic["luot_xem"],
                    "Lượt thích": comic["luot_thich"],
                    "Lượt theo dõi": comic["luot_theo_doi"],
                    "Điểm cơ bản": round(comic.get("base_rating", 0), 2),
                    "Điểm sentiment": round(comic.get("sentiment_rating", 0), 2),
                    "Điểm tổng hợp": round(comic.get("comprehensive_rating", 0), 2)
                })
            
            comics_df = pd.DataFrame(comics_data)
            
            # Tạo DataFrame cho comment
            comments_data = []
            
            for comic in analysis_results:
                comic_comments = comic.get("comments", [])
                
                for comment in comic_comments:
                    comments_data.append({
                        "Tên truyện": comic["ten_truyen"],
                        "Người bình luận": comment["ten_nguoi_binh_luan"],
                        "Nội dung bình luận": comment["noi_dung"],
                        "Cảm xúc": comment["sentiment"],
                        "Điểm cảm xúc": round(comment["sentiment_score"], 2)
                    })
            
            comments_df = pd.DataFrame(comments_data)
            
            # Tạo Excel writer
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                comics_df.to_excel(writer, sheet_name="Đánh giá truyện", index=False)
                comments_df.to_excel(writer, sheet_name="Chi tiết bình luận", index=False)
            
            logger.info(f"Đã xuất kết quả ra file: {filename}")
            
        except Exception as e:
            logger.error(f"Lỗi khi xuất kết quả ra Excel: {str(e)}")