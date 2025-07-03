import logging
import numpy as np

logger = logging.getLogger(__name__)

class Truyentranh3qRatingCalculator:
    """Tính điểm đánh giá cơ bản không sử dụng dữ liệu sentiment"""
    
    @staticmethod
    def calculate(comic):
        """
        Tính điểm đánh giá cơ bản cho một truyện dựa trên số liệu định lượng
        
        Args:
            comic: Dict chứa thông tin truyện
            
        Returns:
            Điểm đánh giá cơ bản (thang điểm 0-10)
        """
        try:
            # Lấy giá trị các trường
            views = comic.get("luot_xem", 0)
            likes = comic.get("luot_thich", 0)
            follows = comic.get("luot_theo_doi", 0)
            chapter_count = comic.get("so_chuong", 0)
            
            # Đảm bảo các giá trị là số
            try:
                views = int(views)
                likes = int(likes)
                follows = int(follows)
                chapter_count = int(chapter_count)
            except (ValueError, TypeError):
                logger.warning(f"Lỗi khi chuyển đổi dữ liệu số cho truyện: {comic.get('ten_truyen')}")
                views = 0
                likes = 0
                follows = 0
                chapter_count = 0
            
            # Tính các chỉ số hiệu quả
            views_per_chapter = views / max(1, chapter_count)  # Lượt xem/chương
            likes_per_chapter = likes / max(1, chapter_count)  # Lượt thích/chương
            follows_per_chapter = follows / max(1, chapter_count)  # Lượt theo dõi/chương
            
            # Chuẩn hóa chỉ số hiệu quả (thang 0-1)
            norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(10000))
            norm_likes_efficiency = min(1.0, np.log10(likes_per_chapter + 1) / np.log10(50))
            norm_follows_efficiency = min(1.0, np.log10(follows_per_chapter + 1) / np.log10(100))
            
            # Chuẩn hóa chỉ số tổng (thang 0-1)
            norm_views_total = min(1.0, np.log10(views + 1) / np.log10(200000))
            norm_likes_total = min(1.0, np.log10(likes + 1) / np.log10(30000))
            norm_follows_total = min(1.0, np.log10(follows + 1) / np.log10(60000))
            
            # Chuẩn hóa số chương
            norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500))
            
            # Tính điểm từ các thành phần
            view_score = (norm_views_total * 0.5) + (norm_views_efficiency * 4.5)
            like_score = (norm_likes_total * 0.5) + (norm_likes_efficiency * 2.5)
            follow_score = (norm_follows_total * 1) + (norm_follows_efficiency * 1)
            chapter_score = norm_chapters * 0
            
            # Điểm cơ bản
            base_rating = view_score + like_score + follow_score + chapter_score
            
            # Đảm bảo điểm nằm trong thang 0-10
            base_rating = min(10.0, max(0.0, base_rating))
            
            # Lưu điểm vào comic để sử dụng sau
            comic["base_rating"] = base_rating
            
            return base_rating
            
        except Exception as e:
            logger.error(f"Lỗi khi tính điểm cơ bản: {str(e)}")
            return 5.0  # Giá trị mặc định