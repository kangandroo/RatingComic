import numpy as np
import logging

logger = logging.getLogger(__name__)

class NetTruyenRatingCalculator:
    @staticmethod
    def calculate(comic_data):
        """
        Tính điểm đánh giá cơ bản cho truyện từ NetTruyen
        
        Args:
            comic_data: Dict chứa thông tin truyện
            
        Returns:
            float: Điểm đánh giá (thang 0-10)
        """
        try:
            # Trích xuất các chỉ số cần thiết
            views = comic_data.get('luot_xem', 0) or 0
            followers = comic_data.get('luot_theo_doi', 0) or 0
            chapter_count = comic_data.get('so_chuong', 0) or 0
            rating_count = comic_data.get('luot_danh_gia', 0) or 0
            
            # Xử lý rating từ chuỗi
            rating_str = comic_data.get('rating', '0')
            if rating_str and '/' in str(rating_str):
                parts = str(rating_str).split('/')
                rating_value = float(parts[0]) / float(parts[1]) * 10
            elif rating_str:
                try:
                    rating_value = float(rating_str)
                    # Dự đoán thang điểm gốc
                    if rating_value > 10:
                        rating_value = rating_value / 10
                    elif rating_value <= 5:
                        rating_value = rating_value * 2
                except:
                    rating_value = 5.0  # Default nếu không thể parse
            else:
                rating_value = 5.0  # Giá trị mặc định
            
            # === CÔNG THỨC NETTRUYEN ===
            
            # 1. Tính các chỉ số hiệu quả
            views_per_chapter = views / max(1, chapter_count)  # Lượt xem/chương
            followers_per_chapter = followers / max(1, chapter_count)  # Lượt theo dõi/chương
            
            # 2. Chuẩn hóa chỉ số hiệu quả (thang 0-1)
            norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(50000)) if views_per_chapter > 0 else 0
            norm_followers_efficiency = min(1.0, np.log10(followers_per_chapter + 1) / np.log10(2000)) if followers_per_chapter > 0 else 0
            
            # 3. Chuẩn hóa chỉ số tổng (thang 0-1)
            norm_views_total = min(1.0, np.log10(views + 1) / np.log10(1000000)) if views > 0 else 0
            norm_followers_total = min(1.0, np.log10(followers + 1) / np.log10(100000)) if followers > 0 else 0
            norm_rating_count = min(1.0, np.log10(rating_count + 1) / np.log10(1000)) if rating_count > 0 else 0
            
            # 4. Chuẩn hóa số chương - giảm ảnh hưởng bằng logarit
            norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500)) if chapter_count > 0 else 0
            
            # 5. Tính điểm từ các thành phần
            view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 2.0)  # Tổng: 3 điểm
            follower_score = (norm_followers_total * 0.5) + (norm_followers_efficiency * 2)  # Tổng: 2.5 điểm
            chapter_score = norm_chapters * 0  # 0.5 điểm
            
            # Điểm đánh giá với trọng số từ số lượng đánh giá
            rating_confidence = min(1.0, rating_count / 100) if rating_count > 0 else 0.1  # Độ tin cậy của rating
            rating_score = (rating_value / 10.0) * 4.5 * rating_confidence  # Tổng: tối đa 4 điểm
            
            # 6. Điểm cơ bản: tổng các thành phần (tối đa 10 điểm)
            base_rating = view_score + follower_score + chapter_score + rating_score
            
            # Đảm bảo điểm nằm trong khoảng 0-10
            base_rating = max(0, min(10, base_rating))
            
            # logger.debug(f"Điểm cơ bản NetTruyen cho '{comic_data.get('ten_truyen')}': {base_rating:.2f}")
            return base_rating
            
        except Exception as e:
            logger.error(f"Lỗi khi tính điểm cơ bản cho NetTruyen: {str(e)}")
            return 5.0  # Giá trị mặc định khi có lỗi