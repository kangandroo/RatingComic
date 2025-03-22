from analysis.truyenqq_rating import TruyenQQRatingCalculator
from analysis.nettruyen_rating import NetTruyenRatingCalculator
from analysis.manhuavn_rating import ManhuavnRatingCalculator
import logging

logger = logging.getLogger(__name__)

class RatingFactory:
    """
    Factory tạo calculator tính điểm phù hợp với từng nguồn dữ liệu
    """
    
    @staticmethod
    def get_calculator(source):
        """
        Lấy calculator tính điểm phù hợp với nguồn
        
        Args:
            source: Tên nguồn dữ liệu
            
        Returns:
            BaseRatingCalculator: Calculator phù hợp
        """
        logger.info(f"Tạo rating calculator cho nguồn: {source}")
        if source == "TruyenQQ":
            return TruyenQQRatingCalculator()
        elif source == "NetTruyen":
            return NetTruyenRatingCalculator()
        elif source == "Manhuavn":
            return ManhuavnRatingCalculator()
        else:
            logger.warning(f"Không có calculator cho nguồn {source}, sử dụng NetTruyenRatingCalculator làm fallback")
            return NetTruyenRatingCalculator()