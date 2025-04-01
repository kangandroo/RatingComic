import logging

logger = logging.getLogger(__name__)

class BaseCrawler:
    """
    Class cơ sở cho tất cả các crawler
    """
    
    def __init__(self, db_manager, config_manager):
        """
        Khởi tạo BaseCrawler
        
        Args:
            db_manager: Database manager instance
            config_manager: Config manager instance
        """
        self.db_manager = db_manager
        self.config_manager = config_manager
        # logger.info("Khởi tạo BaseCrawler")
    
    def crawl_basic_data(self, progress_callback=None):
        """
        Crawl dữ liệu cơ bản từ trang web
        
        Args:
            progress_callback: Callback để cập nhật tiến trình
            
        Returns:
            dict: Kết quả crawl (count, time_taken, website)
        """
        raise NotImplementedError("Các lớp con phải implement phương thức này")
    
    def crawl_comments(self, comic):
        """
        Crawl bình luận cho một truyện cụ thể
        
        Args:
            comic: Dictionary chứa thông tin truyện
            
        Returns:
            list: Danh sách bình luận
        """
        raise NotImplementedError("Các lớp con phải implement phương thức này")