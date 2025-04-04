from analysis.base_rating import BaseRatingCalculator
import numpy as np
import logging

logger = logging.getLogger(__name__)

class ManhuavnRatingCalculator(BaseRatingCalculator):
    """
    Calculator tính điểm đánh giá cho truyện từ nguồn Manhuavn
    """
    
    def extract_number(self, text_value):
        """Trích xuất số từ chuỗi"""
        if not text_value or text_value == 'N/A':
            return 0
            
        if isinstance(text_value, (int, float)):
            return int(text_value)
            
        # Loại bỏ ký tự không phải số
        text_value = str(text_value).strip()
        try:
            # Xử lý hậu tố K và M
            if 'K' in text_value.upper():
                num_part = text_value.upper().replace('K', '')
                return int(float(num_part) * 1000)
            elif 'M' in text_value.upper():
                num_part = text_value.upper().replace('M', '')
                return int(float(num_part) * 1000000)
            else:
                return int(''.join(filter(str.isdigit, text_value)) or 0)
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất số từ '{text_value}': {e}")
            return 0
    
    def calculate(self, comic):
        """
        Tính điểm đánh giá dựa trên dữ liệu từ Manhuavn
        
        Args:
            comic: Dictionary chứa dữ liệu truyện
            
        Returns:
            float: Điểm đánh giá (thang điểm 0-10)
        """
        try:
            # Trích xuất các chỉ số cần thiết
            views = self.extract_number(comic.get('luot_xem', 0))
            followers = self.extract_number(comic.get('luot_theo_doi', 0))
            
            # Xử lý rating và rating_count
            rating_str = comic.get('danh_gia', 'N/A')
            rating_count = self.extract_number(comic.get('luot_danh_gia', 0))
            
            chapter_count = self.extract_number(comic.get('so_chuong', 0))
            
            # Xử lý rating từ chuỗi
            if rating_str != 'N/A' and '/' in rating_str:
                parts = rating_str.split('/')
                rating_value = float(parts[0]) / float(parts[1]) * 10
            elif rating_str != 'N/A':
                try:
                    rating_value = float(rating_str)
                    # Dự đoán thang điểm gốc
                    if rating_value > 10:
                        rating_value = rating_value / 10
                    elif rating_value > 5:
                        rating_value = rating_value
                    else:
                        rating_value = rating_value * 2
                except:
                    rating_value = 5.0  # Default nếu không thể parse
            else:
                rating_value = 5.0  # Giá trị mặc định
                
            # === CÔNG THỨC MỚI ===
            
            # 1. Tính các chỉ số hiệu quả
            views_per_chapter = views / max(1, chapter_count)  # Lượt xem/chương
            followers_per_chapter = followers / max(1, chapter_count)  # Lượt theo dõi/chương
            
            # 2. Chuẩn hóa chỉ số hiệu quả (thang 0-1)
            norm_views_efficiency = min(1.0, np.log10(views_per_chapter + 1) / np.log10(1500)) if views_per_chapter > 0 else 0
            norm_followers_efficiency = min(1.0, np.log10(followers_per_chapter + 1) / np.log10(100)) if followers_per_chapter > 0 else 0
            
            # 3. Chuẩn hóa chỉ số tổng (thang 0-1)
            norm_views_total = min(1.0, np.log10(views + 1) / np.log10(500000)) if views > 0 else 0
            norm_followers_total = min(1.0, np.log10(followers + 1) / np.log10(30000)) if followers > 0 else 0
            norm_rating_count = min(1.0, np.log10(rating_count + 1) / np.log10(1000)) if rating_count > 0 else 0
            
            # 4. Chuẩn hóa số chương - giảm ảnh hưởng bằng logarit
            norm_chapters = min(1.0, np.log10(chapter_count + 1) / np.log10(500)) if chapter_count > 0 else 0
            
            # 5. Tính điểm từ các thành phần
            view_score = (norm_views_total * 1.0) + (norm_views_efficiency * 1.5)  
            follower_score = (norm_followers_total * 0.5) + (norm_followers_efficiency * 3)  
            chapter_score = norm_chapters * 0  
            
            # Điểm đánh giá với trọng số từ số lượng đánh giá
            rating_confidence = min(1.0, rating_count / (0.01 * max(1,followers))) if rating_count > 0 else 0.1  
            rating_score = (rating_value / 10.0) * 4 * rating_confidence 
            
            # 6. Điểm cơ bản: thành phần định lượng
            base_rating = view_score + follower_score + chapter_score + rating_score
            
            # Đảm bảo điểm nằm trong thang 0-10
            base_rating = min(10.0, max(0.0, base_rating))
            
            # logger.info(f"Tính điểm ManhuavnRatingCalculator: view_score={view_score:.2f}, follower_score={follower_score:.2f}, " 
            #            f"chapter_score={chapter_score:.2f}, rating_score={rating_score:.2f}, base_rating={base_rating:.2f}")
            
            return base_rating
            
        except Exception as e:
            logger.error(f"Lỗi khi tính điểm Manhuavn: {str(e)}")
            return 5.0  # Giá trị mặc định khi có lỗi