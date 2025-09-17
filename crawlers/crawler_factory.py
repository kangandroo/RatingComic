from crawlers.truyenqq_crawler import TruyenQQCrawler
from crawlers.nettruyen_crawler import NetTruyenCrawler
from crawlers.manhuavn_crawler import ManhuavnCrawler
from crawlers.truyentranh3q_crawler import Truyentranh3qCrawler
import logging

logger = logging.getLogger(__name__)

class CrawlerFactory:
    """
    Factory tạo crawler phù hợp với từng website
    """
    
    # Biến static để lưu config_manager
    config_manager = None
    
    @classmethod
    def initialize(cls, config_manager):
        """
        Khởi tạo CrawlerFactory với config_manager
        
        Args:
            config_manager: Config manager instance
        """
        cls.config_manager = config_manager
        logger.info("Đã khởi tạo CrawlerFactory với config_manager")
    
    @classmethod
    def create_crawler(cls, crawler_type, db_manager, config_manager=None, **kwargs):
        """
        Tạo crawler dựa trên loại được chỉ định
        
        Args:
            crawler_type (str): Loại crawler ('TruyenQQ', 'NetTruyen', 'Manhuavn')
            db_manager: Database manager instance
            config_manager: Config manager instance (nếu không cung cấp, sử dụng cls.config_manager)
            **kwargs: Tham số tuỳ chọn cho crawler:
                - start_page (int): Trang bắt đầu (mặc định: 1)
                - end_page (int): Trang kết thúc (mặc định: start_page + max_pages - 1)
                - max_pages (int): Số trang tối đa (mặc định: 10)
                - worker_count (int): Số worker (mặc định: 5)
                - base_url (str): URL cơ sở
            
        Returns:
            BaseCrawler: Instance của crawler tương ứng
        """
        logger.info(f"Đang tạo crawler loại: {crawler_type}")
        
        # Sử dụng config_manager từ tham số hoặc từ class variable
        config_manager = config_manager or cls.config_manager
        if not config_manager:
            raise ValueError("config_manager không được cung cấp và chưa được khởi tạo")
        
        # Lấy thông tin trang
        start_page = kwargs.get('start_page', config_manager.get('start_page', 1))
        end_page = kwargs.get('end_page', None)
        max_pages = kwargs.get('max_pages', config_manager.get('max_pages', 10))
        
        # Nếu không có end_page, tính từ start_page và max_pages
        if end_page is None:
            end_page = start_page + max_pages - 1
        
        # Đảm bảo logic đúng
        if end_page < start_page:
            end_page = start_page
        
        # Tính lại max_pages dựa trên start_page và end_page
        actual_max_pages = end_page - start_page + 1
        
        # Lấy thông tin khác
        worker_count = kwargs.get('worker_count', config_manager.get('worker_count', 5))
        
        # Các website được hỗ trợ từ config
        supported_websites = config_manager.get_supported_websites()
        
        # Thông tin cấu hình chung cho tất cả crawler
        common_kwargs = {
            'db_manager': db_manager,
            'config_manager': config_manager,
            'start_page': start_page,
            'end_page': end_page,
            'max_pages': actual_max_pages,
            'worker_count': worker_count
        }
        
        # logger.info(f"Tham số crawler: start_page={start_page}, end_page={end_page}, max_pages={actual_max_pages}, worker_count={worker_count}")
        
        if crawler_type == "TruyenQQ":
            return TruyenQQCrawler(
                **common_kwargs,
                base_url=kwargs.get('base_url', supported_websites.get('TruyenQQ', 'https://truyenqqgo.com'))
            )
        elif crawler_type == "NetTruyen":
            return NetTruyenCrawler(
                **common_kwargs,
                base_url=kwargs.get('base_url', supported_websites.get('NetTruyen', 'https://nettruyenvia.com'))
            )
        elif crawler_type == "Manhuavn":
            return ManhuavnCrawler(
                **common_kwargs,
                base_url=kwargs.get('base_url', supported_websites.get('Manhuavn', 'https://manhuavn.top'))
            )
        elif crawler_type == "Truyentranh3q":
            return Truyentranh3qCrawler(
                **common_kwargs,
                base_url=kwargs.get('base_url', supported_websites.get('Truyentranh3q', 'https://truyentranh3q.com'))
            )
        else:
            logger.error(f"Không hỗ trợ loại crawler: {crawler_type}")
            raise ValueError(f"Không hỗ trợ loại crawler: {crawler_type}")