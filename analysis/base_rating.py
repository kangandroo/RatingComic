class BaseRatingCalculator:
    """
    Class cơ sở cho các calculator tính điểm truyện
    """
    
    def calculate(self, comic):
        """
        Tính điểm đánh giá cho truyện
        
        Args:
            comic: Dictionary chứa dữ liệu truyện
            
        Returns:
            float: Điểm đánh giá (thang điểm 0-10)
        """
        raise NotImplementedError("Các lớp con phải implement phương thức này")