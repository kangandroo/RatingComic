import logging
import re

logger = logging.getLogger(__name__)

class DataNormalizer:
    """
    Chuẩn hóa dữ liệu từ nhiều nguồn khác nhau
    """
    
    @staticmethod
    def normalize_comic_data(comic, source):
        """
        Chuẩn hóa dữ liệu truyện từ nhiều nguồn khác nhau
        
        Args:
            comic: Dictionary chứa dữ liệu truyện
            source: Nguồn dữ liệu
            
        Returns:
            dict: Dữ liệu đã được chuẩn hóa
        """
        try:
            logger.info(f"Chuẩn hóa dữ liệu truyện từ nguồn: {source}")
            
            # Tạo một bản sao để không ảnh hưởng đến dữ liệu gốc
            normalized = comic.copy()
            
            # Đảm bảo tất cả các trường cơ bản đều tồn tại
            normalized.setdefault("ten_truyen", "")
            normalized.setdefault("tac_gia", "")
            normalized.setdefault("the_loai", "")
            normalized.setdefault("mo_ta", "")
            normalized.setdefault("link_truyen", "")
            normalized.setdefault("so_chuong", 0)
            normalized.setdefault("luot_xem", 0)
            normalized.setdefault("trang_thai", "")
            normalized.setdefault("nguon", source)
            
            # Chuyển đổi các kiểu dữ liệu
            if isinstance(normalized.get("so_chuong"), str):
                try:
                    normalized["so_chuong"] = int(re.sub(r'[^\d]', '', normalized["so_chuong"]) or 0)
                except:
                    normalized["so_chuong"] = 0
            
            if isinstance(normalized.get("luot_xem"), str):
                normalized["luot_xem"] = DataNormalizer.extract_number(normalized["luot_xem"])
            
            # Xử lý theo từng nguồn
            if source == "TruyenQQ":
                normalized.setdefault("luot_thich", 0)
                normalized.setdefault("luot_theo_doi", 0)
                normalized.setdefault("so_binh_luan", 0)
            
            elif source == "NetTruyen":
                normalized.setdefault("luot_thich", 0)
                normalized.setdefault("luot_theo_doi", 0)
                normalized.setdefault("rating", "")
                normalized.setdefault("luot_danh_gia", 0)
                normalized.setdefault("so_binh_luan", 0)
                
                # Xử lý rating
                if isinstance(normalized.get("rating"), str) and '/' in normalized.get("rating", ""):
                    parts = normalized["rating"].split('/')
                    normalized["rating"] = parts[0].strip()
            
            elif source == "Manhuavn":
                normalized.setdefault("luot_theo_doi", 0)
                normalized.setdefault("danh_gia", "")
                normalized.setdefault("luot_danh_gia", 0)
                
                # Chuẩn hóa lượt theo dõi
                if isinstance(normalized.get("luot_theo_doi"), str):
                    normalized["luot_theo_doi"] = DataNormalizer.extract_number(normalized["luot_theo_doi"])
                
                # Chuẩn hóa lượt đánh giá
                if isinstance(normalized.get("luot_danh_gia"), str):
                    normalized["luot_danh_gia"] = DataNormalizer.extract_number(normalized["luot_danh_gia"])
            elif source == "Truyentranh3q":
                normalized.setdefault("luot_thich", 0)
                normalized.setdefault("luot_theo_doi", 0)
                normalized.setdefault("so_binh_luan", 0)

            return normalized
            
        except Exception as e:
            logger.error(f"Lỗi khi chuẩn hóa dữ liệu truyện: {str(e)}")
            return comic
    
    @staticmethod
    def extract_number(text_value):
        """Trích xuất số từ chuỗi"""
        if not text_value:
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