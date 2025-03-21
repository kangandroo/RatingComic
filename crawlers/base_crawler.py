from abc import ABC, abstractmethod

class BaseCrawler(ABC):
    """Lớp cơ sở cho tất cả crawler"""
    
    def __init__(self, db_manager, base_url=None, max_pages=None):
        """
        Khởi tạo crawler
        
        Args:
            db_manager: Database manager để lưu dữ liệu
            base_url: URL gốc của website
            max_pages: Số trang tối đa để crawl (None để không giới hạn)
        """
        self.db_manager = db_manager
        self.base_url = base_url
        self.max_pages = max_pages
    
    @abstractmethod
    def crawl_basic_data(self, progress_callback=None):
        """
        Crawl dữ liệu cơ bản của truyện (không bao gồm comment)
        
        Args:
            progress_callback: Callback để cập nhật tiến trình
            
        Returns:
            Dict chứa kết quả crawl
        """
        pass
    
    @abstractmethod
    def crawl_comments(self, comic):
        """
        Crawl comment cho một truyện cụ thể
        
        Args:
            comic: Thông tin truyện cần crawl comment
            
        Returns:
            List các comment
        """
        pass