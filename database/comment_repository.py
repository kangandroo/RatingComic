import logging
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class CommentRepository:
    """Lớp repository cho xử lý dữ liệu bình luận"""
    
    def __init__(self, db_manager):
        """
        Khởi tạo Comment Repository
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    def get_by_comic_id(self, comic_id):
        """
        Lấy tất cả bình luận của một truyện
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            List các bình luận
        """
        return self.db_manager.get_comic_comments(comic_id)
    
    def get_by_id(self, comment_id):
        """
        Lấy thông tin bình luận theo ID
        
        Args:
            comment_id: ID của bình luận
            
        Returns:
            Dict chứa thông tin bình luận hoặc None nếu không tìm thấy
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM comments WHERE id = ?", (comment_id,))
            comment = cursor.fetchone()
            
            conn.close()
            
            return comment
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy bình luận ID {comment_id}: {str(e)}")
            return None
    
    def get_positive_comments(self, comic_id, limit=10):
        """
        Lấy danh sách bình luận tích cực của một truyện
        
        Args:
            comic_id: ID của truyện
            limit: Số lượng bình luận cần lấy
            
        Returns:
            List các bình luận tích cực sắp xếp theo điểm sentiment giảm dần
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM comments 
                WHERE comic_id = ? AND sentiment = 'positive' 
                ORDER BY sentiment_score DESC 
                LIMIT ?
            """, (comic_id, limit))
            
            comments = cursor.fetchall()
            
            conn.close()
            
            return comments
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy bình luận tích cực: {str(e)}")
            return []
    
    def get_negative_comments(self, comic_id, limit=10):
        """
        Lấy danh sách bình luận tiêu cực của một truyện
        
        Args:
            comic_id: ID của truyện
            limit: Số lượng bình luận cần lấy
            
        Returns:
            List các bình luận tiêu cực sắp xếp theo điểm sentiment tăng dần
        """
        try:
            conn = self.db_manager.get_connection()
            conn.row_factory = lambda cursor, row: {
                col[0]: row[idx] for idx, col in enumerate(cursor.description)
            }
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM comments 
                WHERE comic_id = ? AND sentiment = 'negative' 
                ORDER BY sentiment_score ASC 
                LIMIT ?
            """, (comic_id, limit))
            
            comments = cursor.fetchall()
            
            conn.close()
            
            return comments
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy bình luận tiêu cực: {str(e)}")
            return []
    
    def save_batch(self, comic_id, comments):
        """
        Lưu một loạt bình luận vào database
        
        Args:
            comic_id: ID của truyện
            comments: List bình luận cần lưu
            
        Returns:
            Số lượng bình luận đã lưu
        """
        if not comments:
            return 0
            
        self.db_manager.save_comments(comic_id, comments)
        return len(comments)
    
    def update_sentiment(self, comment_id, sentiment, score):
        """
        Cập nhật thông tin sentiment cho một bình luận
        
        Args:
            comment_id: ID của bình luận
            sentiment: Loại sentiment (positive, negative, neutral)
            score: Điểm sentiment
            
        Returns:
            True nếu cập nhật thành công, False nếu không
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE comments 
                SET sentiment = ?, sentiment_score = ? 
                WHERE id = ?
            """, (sentiment, score, comment_id))
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật sentiment: {str(e)}")
            return False
    
    def get_sentiment_stats(self, comic_id):
        """
        Lấy thống kê sentiment cho một truyện
        
        Args:
            comic_id: ID của truyện
            
        Returns:
            Dict chứa thống kê
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # Tổng số bình luận
            cursor.execute("SELECT COUNT(*) FROM comments WHERE comic_id = ?", (comic_id,))
            total = cursor.fetchone()[0]
            
            # Số lượng mỗi loại sentiment
            cursor.execute("SELECT COUNT(*) FROM comments WHERE comic_id = ? AND sentiment = 'positive'", (comic_id,))
            positive = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM comments WHERE comic_id = ? AND sentiment = 'negative'", (comic_id,))
            negative = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM comments WHERE comic_id = ? AND sentiment = 'neutral'", (comic_id,))
            neutral = cursor.fetchone()[0]
            
            # Điểm sentiment trung bình
            cursor.execute("SELECT AVG(sentiment_score) FROM comments WHERE comic_id = ?", (comic_id,))
            avg_score = cursor.fetchone()[0] or 0.5
            
            conn.close()
            
            # Tính tỷ lệ
            positive_ratio = positive / total if total > 0 else 0
            negative_ratio = negative / total if total > 0 else 0
            neutral_ratio = neutral / total if total > 0 else 0
            
            return {
                "total": total,
                "positive": positive,
                "negative": negative,
                "neutral": neutral,
                "positive_ratio": positive_ratio,
                "negative_ratio": negative_ratio,
                "neutral_ratio": neutral_ratio,
                "avg_score": avg_score
            }
            
        except Exception as e:
            logger.error(f"Lỗi khi lấy thống kê sentiment: {str(e)}")
            return {
                "total": 0,
                "positive": 0,
                "negative": 0,
                "neutral": 0,
                "positive_ratio": 0,
                "negative_ratio": 0,
                "neutral_ratio": 0,
                "avg_score": 0.5
            }