import logging
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class ComicRepository:
    """Lớp repository cho xử lý dữ liệu truyện tranh"""
    
    def __init__(self, db_manager):
        """
        Khởi tạo Comic Repository
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    def get_all(self):
        """
        Lấy tất cả truyện từ database
        
        Returns:
            List các truyện
        """
        return self.db_manager.get_all_comics()
    
    def get_by_id(self, comic_id):
        """
        Lấy thông tin truyện theo ID
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            Dict chứa thông tin truyện hoặc None nếu không tìm thấy
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM comics WHERE id = ?", (comic_id,))
            comic = cursor.fetchone()
            
            conn.close()
            
            return comic
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy truyện ID {comic_id}: {str(e)}")
            return None
    
    def get_by_filters(self, genre=None, min_views=0, min_comments=0, status=None):
        """
        Lấy danh sách truyện theo bộ lọc
        
        Args:
            genre: Thể loại truyện (None để không lọc)
            min_views: Số lượt xem tối thiểu
            min_comments: Số bình luận tối thiểu
            status: Trạng thái truyện (None để không lọc)
            
        Returns:
            List các truyện thỏa mãn điều kiện
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            query = "SELECT * FROM comics WHERE luot_xem >= ? AND so_binh_luan >= ?"
            params = [min_views, min_comments]
            
            if genre and genre != "Tất cả thể loại":
                query += " AND the_loai LIKE ?"
                params.append(f"%{genre}%")
                
            if status:
                query += " AND trang_thai = ?"
                params.append(status)
                
            cursor.execute(query, params)
            comics = cursor.fetchall()
            
            conn.close()
            
            return comics
            
        except Exception as e:
            logger.error(f"Lỗi khi lọc truyện: {str(e)}")
            return []
    
    def get_top_comics(self, limit=10, order_by="luot_xem"):
        """
        Lấy top truyện theo tiêu chí
        
        Args:
            limit: Số lượng truyện cần lấy
            order_by: Tiêu chí sắp xếp (luot_xem, luot_thich, so_binh_luan)
            
        Returns:
            List truyện sắp xếp theo tiêu chí
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            # Đảm bảo order_by là một cột hợp lệ
            valid_columns = ["luot_xem", "luot_thich", "luot_theo_doi", "so_binh_luan", "so_chuong"]
            if order_by not in valid_columns:
                order_by = "luot_xem"
                
            query = f"SELECT * FROM comics ORDER BY {order_by} DESC LIMIT ?"
            cursor.execute(query, (limit,))
            
            comics = cursor.fetchall()
            
            conn.close()
            
            return comics
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy top truyện: {str(e)}")
            return []
    
    def save(self, comic_data):
        """
        Lưu thông tin truyện vào database
        
        Args:
            comic_data: Dict chứa thông tin truyện
            
        Returns:
            ID của truyện đã lưu
        """
        return self.db_manager.save_comic(comic_data)
    
    def delete(self, comic_id):
        """
        Xóa truyện khỏi database
        
        Args:
            comic_id: ID của truyện cần xóa
            
        Returns:
            True nếu xóa thành công, False nếu không
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # Xóa comment trước
            cursor.execute("DELETE FROM comments WHERE comic_id = ?", (comic_id,))
            
            # Xóa truyện
            cursor.execute("DELETE FROM comics WHERE id = ?", (comic_id,))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Đã xóa truyện ID: {comic_id}")
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi xóa truyện ID {comic_id}: {str(e)}")
            return False