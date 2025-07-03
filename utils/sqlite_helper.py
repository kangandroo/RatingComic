import os
import sqlite3
import logging
import threading
import queue
from typing import List, Dict, Any, Optional
import time

logger = logging.getLogger(__name__)

class SQLiteHelper:
    """
    Helper class để thực hiện các thao tác SQLite an toàn với thread và tối ưu performance
    """
    
    def __init__(self, db_folder, pool_size=5):
        """
        Khởi tạo SQLiteHelper
        
        Args:
            db_folder: Thư mục chứa database
            pool_size: Số lượng kết nối tối đa trong pool cho mỗi nguồn
        """
        self.db_folder = db_folder
        self.pool_size = pool_size
        self.thread_local = threading.local()
        self.connection_pools = {}  # Pool connections theo nguồn
        self.pool_locks = {}  # Lock để bảo vệ pool
        
        # Tạo thư mục nếu chưa tồn tại
        os.makedirs(db_folder, exist_ok=True)
        
        # Định nghĩa schema cho từng nguồn
        self.schemas = {
            "TruyenQQ": self._get_truyenqq_schema(),
            "NetTruyen": self._get_nettruyen_schema(),
            "Manhuavn": self._get_manhuavn_schema() ,
            "Truyentranh3q": self._get_truyentranh3q_schema()
        }
        
        # Khởi tạo locks cho từng nguồn
        for source in self.schemas.keys():
            self.pool_locks[source] = threading.Lock()
        
        # logger.info(f"Khởi tạo SQLiteHelper với db_folder: {db_folder}, pool_size: {pool_size}")
    
    # [Các phương thức _get_*_schema() giữ nguyên]
    def _get_truyenqq_schema(self):
        """Lấy schema cho TruyenQQ database"""
        # [Giữ nguyên code hiện tại]
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
        # [Giữ nguyên code hiện tại]
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
        # [Giữ nguyên code hiện tại]
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
    
    def _get_truyentranh3q_schema(self):
        """Lấy schema cho Truyentranh3q database"""
        # [Giữ nguyên code hiện tại]
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
                    nguon TEXT DEFAULT 'Truyentranh3q',
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
        # [Giữ nguyên code hiện tại]
        # Map nguồn dữ liệu đến tên file
        db_files = {
            "TruyenQQ": "truyenqq.db",
            "NetTruyen": "nettruyen.db",
            "Manhuavn": "manhuavn.db",
            "Truyentranh3q": "truyentranh3q.db"
        }
        
        if source_name not in db_files:
            raise ValueError(f"Nguồn không được hỗ trợ: {source_name}")
        
        return os.path.join(self.db_folder, db_files[source_name])
    
    def _initialize_pool(self, source_name):
        """Khởi tạo pool cho nguồn dữ liệu"""
        with self.pool_locks[source_name]:
            if source_name not in self.connection_pools:
                self.connection_pools[source_name] = queue.Queue()
                db_file = self._get_db_file(source_name)
                
                # Tạo các kết nối cho pool
                for _ in range(self.pool_size):
                    conn = sqlite3.connect(db_file, check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                    
                    # Tạo bảng nếu cần
                    cursor = conn.cursor()
                    for table_name, schema in self.schemas[source_name].items():
                        cursor.execute(schema)
                    conn.commit()
                    
                    self.connection_pools[source_name].put(conn)
                
                logger.info(f"Khởi tạo connection pool cho nguồn: {source_name} với {self.pool_size} connections")
    
    def _get_connection_from_pool(self, source_name):
        """Lấy kết nối từ pool, khởi tạo pool nếu cần"""
        if source_name not in self.connection_pools:
            self._initialize_pool(source_name)
        
        try:
            # Lấy kết nối từ pool với timeout 2 giây
            with self.pool_locks[source_name]:
                if self.connection_pools[source_name].empty():
                    # Tạo thêm kết nối nếu pool rỗng
                    db_file = self._get_db_file(source_name)
                    conn = sqlite3.connect(db_file, check_same_thread=False)
                    conn.row_factory = sqlite3.Row
                    return conn
                else:
                    return self.connection_pools[source_name].get(block=False)
                
        except queue.Empty:
            # Nếu pool rỗng (timeout), tạo kết nối mới
            logger.warning(f"Connection pool cho {source_name} rỗng, tạo kết nối mới")
            db_file = self._get_db_file(source_name)
            conn = sqlite3.connect(db_file, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            return conn
        except Exception as e:
            logger.error(f"Lỗi khi lấy kết nối từ pool: {str(e)}")
            # Fallback tới cách truyền thống
            return self._get_connection(source_name)
    
    def _return_connection_to_pool(self, conn, source_name):
        """Trả kết nối về pool nếu có thể"""
        if source_name in self.connection_pools:
            with self.pool_locks[source_name]:
                try:
                    # Chỉ trả về pool nếu chưa đầy
                    if self.connection_pools[source_name].qsize() < self.pool_size:
                        self.connection_pools[source_name].put(conn, block=False)
                    else:
                        conn.close()
                except:
                    # Nếu không thể trả về pool, đóng kết nối
                    conn.close()
        else:
            conn.close()
    
    def _get_connection(self, source_name):
        """
        Phương thức cũ để lấy connection - giữ lại để tương thích với code cũ
        
        Args:
            source_name: Tên nguồn dữ liệu
            
        Returns:
            sqlite3.Connection: SQLite connection
        """
        # Thử lấy từ pool trước
        try:
            return self._get_connection_from_pool(source_name)
        except:
            # Fallback tới cách cũ nếu có lỗi
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
    
    def save_comics_batch(self, comics_list, source_name, timeout=30):
        """
        Lưu nhiều truyện cùng lúc trong một transaction với timeout
        
        Args:
            comics_list: Danh sách dữ liệu truyện
            source_name: Tên nguồn
            timeout: Thời gian tối đa (giây) để thực hiện thao tác, mặc định 60 giây
            
        Returns:
            list: Danh sách ID của các truyện đã lưu
        """
        if not comics_list:
            return []
        
        start_time = time.time()  # Ghi lại thời điểm bắt đầu
        conn = None
        
        try:
            # Kiểm tra timeout trước khi lấy kết nối
            if time.time() - start_time > timeout:
                logger.warning(f"Timeout khi lưu batch ({timeout}s)")
                return []
                
            conn = self._get_connection_from_pool(source_name)
            cursor = conn.cursor()
            comic_ids = []
            
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            # Giới hạn số lượng mỗi batch nếu danh sách quá lớn
            batch_size = min(len(comics_list), 1000)  # Giới hạn kích thước batch
            processed_comics = 0
            
            for comic_data in comics_list:
                # Kiểm tra timeout định kỳ 
                processed_comics += 1
                if processed_comics % 50 == 0 and time.time() - start_time > timeout:
                    logger.warning(f"Timeout sau khi lưu {processed_comics}/{len(comics_list)} truyện ({timeout}s)")
                    conn.rollback()
                    return comic_ids[:processed_comics-50]  # Trả về ID các truyện đã được xử lý trước batch hiện tại
                
                # Điều chỉnh query tùy theo nguồn dữ liệu (giữ nguyên code hiện tại)
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
                elif source_name == "Truyentranh3q":
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
                        comic_data.get("nguon", "Truyentranh3q")
                    )
                else:
                    continue
                
                cursor.execute(query, params)
                
                # Lấy ID của truyện vừa thêm
                cursor.execute("SELECT id FROM comics WHERE link_truyen = ?", (comic_data.get("link_truyen", ""),))
                result = cursor.fetchone()
                if result:
                    comic_ids.append(result["id"])
            
            # Kiểm tra timeout trước khi commit
            if time.time() - start_time > timeout:
                logger.warning(f"Timeout trước khi commit ({timeout}s)")
                conn.rollback()
                return comic_ids  # Có thể một số ID đã được lấy nhưng chưa commit
            
            # Commit tất cả cùng lúc
            conn.commit()
            # logger.info(f"Đã lưu batch {len(comics_list)} truyện vào {source_name} trong {time.time() - start_time:.2f}s")
            
            return comic_ids
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu batch truyện vào database: {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception as rollback_error:
                    logger.error(f"Lỗi khi rollback: {rollback_error}")
            return comic_ids  # Trả về danh sách ID đã thu thập được (nếu có)
        finally:
            # Kiểm tra xem conn có tồn tại không và trả lại pool
            if conn:
                try:
                    self._return_connection_to_pool(conn, source_name)
                except Exception as pool_error:
                    logger.error(f"Lỗi khi trả kết nối về pool: {pool_error}")
                    try:
                        conn.close()  # Cố gắng đóng kết nối nếu không thể trả về pool
                    except:
                        pass
    
    def save_comments_batch(self, comments_batch, source_name):
        """
        Lưu nhiều bình luận từ nhiều truyện cùng lúc
        
        Args:
            comments_batch: Dictionary với key là comic_id và value là list comments
            source_name: Tên nguồn
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        if not comments_batch:
            return True
            
        conn = self._get_connection_from_pool(source_name)
        cursor = conn.cursor()
        
        try:
            # Bắt đầu transaction
            conn.execute("BEGIN TRANSACTION")
            
            total_comments = 0
            
            for comic_id, comments_data in comments_batch.items():
                if not comments_data:
                    continue
                    
                # Xóa comments cũ
                cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
                
                # Chuẩn bị dữ liệu cho executemany
                comments_params = []
                for comment in comments_data:
                    comments_params.append((
                        comic_id,
                        comment.get("ten_nguoi_binh_luan", ""),
                        comment.get("noi_dung", ""),
                        comment.get("sentiment", ""),
                        comment.get("sentiment_score", 0)
                    ))
                
                # Thêm comments mới với executemany
                cursor.executemany('''
                    INSERT INTO comments 
                    (comic_id, ten_nguoi_binh_luan, noi_dung, sentiment, sentiment_score)
                    VALUES (?, ?, ?, ?, ?)
                ''', comments_params)
                
                total_comments += len(comments_data)
            
            # Commit tất cả cùng lúc
            conn.commit()
            logger.info(f"Đã lưu batch {total_comments} bình luận cho {len(comments_batch)} truyện vào {source_name}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu batch bình luận vào database: {e}")
            conn.rollback()
            return False
        finally:
            self._return_connection_to_pool(conn, source_name)
    
    # Cập nhật phương thức hiện tại để sử dụng connection pool
    def save_comic_to_db(self, comic_data, source_name):
        """
        Lưu truyện vào database an toàn với thread
        
        """
        # Gọi save_comics_batch với một truyện
        result = self.save_comics_batch([comic_data], source_name)
        return result[0] if result else None
    
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
        # Gọi save_comments_batch với một dictionary chứa một comic_id
        return self.save_comments_batch({comic_id: comments_data}, source_name)
    
    # [Các phương thức get_* cập nhật để sử dụng connection pool]
    def get_all_comics(self, source_name):
        """
        Lấy tất cả truyện từ một nguồn
        
        Args:
            source_name: Tên nguồn dữ liệu
            
        Returns:
            list: Danh sách truyện
        """
        conn = self._get_connection_from_pool(source_name)
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comics")
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
            return []
        finally:
            self._return_connection_to_pool(conn, source_name)
    
    def get_comic_by_id(self, comic_id, source_name):
        """
        Lấy thông tin truyện theo ID
        
        Args:
            comic_id: ID của truyện
            source_name: Tên nguồn dữ liệu
            
        Returns:
            dict: Thông tin truyện
        """
        conn = self._get_connection_from_pool(source_name)
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comics WHERE id = ?", (comic_id,))
            row = cursor.fetchone()
            
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin truyện: {e}")
            return None
        finally:
            self._return_connection_to_pool(conn, source_name)
    
    def get_comments_by_comic_id(self, comic_id, source_name):
        """
        Lấy bình luận của truyện theo ID
        
        Args:
            comic_id: ID của truyện
            source_name: Tên nguồn dữ liệu
            
        Returns:
            list: Danh sách bình luận
        """
        conn = self._get_connection_from_pool(source_name)
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM comments WHERE comic_id = ?", (comic_id,))
            rows = cursor.fetchall()
            
            # Chuyển từ Row sang Dict
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy bình luận: {e}")
            return []
        finally:
            self._return_connection_to_pool(conn, source_name)
    
    def close_all_connections(self):
        """Đóng tất cả kết nối và làm sạch pools"""
        # Đóng kết nối thread-local
        if hasattr(self.thread_local, "connections"):
            for conn in self.thread_local.connections.values():
                try:
                    conn.close()
                except Exception as e:
                    logger.debug(f"Lỗi khi đóng connection: {e}")
            
            self.thread_local.connections = {}
        
        # Đóng kết nối trong pools
        for source_name, pool in self.connection_pools.items():
            closed_count = 0
            with self.pool_locks[source_name]:
                while not pool.empty():
                    try:
                        conn = pool.get_nowait()
                        conn.close()
                        closed_count += 1
                    except:
                        pass
            
            logger.info(f"Đã đóng {closed_count} kết nối từ pool cho {source_name}")