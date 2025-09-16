import logging
from crawlers.comment_crawler import CommentCrawler

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
        
        # Khởi tạo comment crawler
        self.comment_crawler = None
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
        Crawl bình luận cho một truyện cụ thể (legacy method - deprecated)
        
        Args:
            comic: Dictionary chứa thông tin truyện
            
        Returns:
            list: Danh sách bình luận
        """
        raise NotImplementedError("Các lớp con phải implement phương thức này")
    
    def crawl_comments_parallel(self, comics_list, progress_callback=None):
        """
        Crawl comments song song cho danh sách truyện
        
        Args:
            comics_list: Danh sách truyện cần crawl comments
            progress_callback: Callback để báo cáo tiến trình
            
        Returns:
            dict: Kết quả crawl
        """
        if not self.comment_crawler:
            website_type = self.__class__.__name__.replace("Crawler", "")
            self.comment_crawler = CommentCrawler(
                website_type=website_type,
                db_path=self.db_manager.db_folder,
                max_workers=self.config_manager.get('comment_workers', 4)
            )
        
        return self.comment_crawler.crawl_comments_parallel(comics_list, progress_callback)