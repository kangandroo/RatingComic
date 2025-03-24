from analysis.truyenqq_rating import TruyenQQRatingCalculator
from analysis.nettruyen_rating import NetTruyenRatingCalculator
from analysis.manhuavn_rating import ManhuavnRatingCalculator
import logging

logger = logging.getLogger(__name__)

class RatingFactory:
    """
    Factory tạo calculator tính điểm phù hợp với từng nguồn dữ liệu
    """
    # Dictionary cache để lưu các calculator đã tạo
    _calculators = {}
    
    @classmethod
    def get_calculator(cls, source):
        """
        Lấy calculator tính điểm phù hợp với nguồn,
        sử dụng cache để tránh tạo mới đối tượng không cần thiết
        
        Args:
            source: Tên nguồn dữ liệu
            
        Returns:
            BaseRatingCalculator: Calculator phù hợp
        """
        
        # Kiểm tra cache trước khi tạo đối tượng mới
        if source not in cls._calculators:
            logger.info(f"Tạo rating calculator cho nguồn: {source}")
            if source == "TruyenQQ":
                cls._calculators[source] = TruyenQQRatingCalculator()
            elif source == "NetTruyen":
                cls._calculators[source] = NetTruyenRatingCalculator()
            elif source == "Manhuavn":
                cls._calculators[source] = ManhuavnRatingCalculator()
            else:
                logger.warning(f"Không có calculator cho nguồn {source}, sử dụng NetTruyenRatingCalculator làm fallback")
                cls._calculators[source] = NetTruyenRatingCalculator()
        
        return cls._calculators[source]
    
    @classmethod
    def clear_cache(cls):
        """
        Xóa cache các calculators khi cần thiết
        (ví dụ: khi thay đổi cấu hình tính điểm)
        """
        cls._calculators.clear()
        logger.debug("Đã xóa cache rating calculators")