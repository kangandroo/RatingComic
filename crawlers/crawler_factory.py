from crawlers.truyenqq_crawler import TruyenQQCrawler
from crawlers.nettruyen_crawler import NetTruyenCrawler
from crawlers.manhuavn_crawler import ManhuavnCrawler
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
            **kwargs: Tham số tuỳ chọn cho crawler
            
        Returns:
            BaseCrawler: Instance của crawler tương ứng
        """
        logger.info(f"Đang tạo crawler loại: {crawler_type}")
        
        # Sử dụng config_manager từ tham số hoặc từ class variable
        config_manager = config_manager or cls.config_manager
        if not config_manager:
            raise ValueError("config_manager không được cung cấp và chưa được khởi tạo")
        
        # Các website được hỗ trợ từ config
        supported_websites = config_manager.get_supported_websites()
        
        if crawler_type == "TruyenQQ":
            return TruyenQQCrawler(
                db_manager,
                config_manager,
                base_url=kwargs.get('base_url', supported_websites.get('TruyenQQ', 'https://truyenqqto.com')),
                max_pages=kwargs.get('max_pages', config_manager.get('max_pages', 10)),
                worker_count=kwargs.get('worker_count', config_manager.get('worker_count', 5))
            )
        elif crawler_type == "NetTruyen":
            return NetTruyenCrawler(
                db_manager,
                config_manager,
                base_url=kwargs.get('base_url', supported_websites.get('NetTruyen', 'https://nettruyenvie.com')),
                max_pages=kwargs.get('max_pages', config_manager.get('max_pages', 10)),
                worker_count=kwargs.get('worker_count', config_manager.get('worker_count', 5))
            )
        elif crawler_type == "Manhuavn":
            return ManhuavnCrawler(
                db_manager,
                config_manager,
                base_url=kwargs.get('base_url', supported_websites.get('Manhuavn', 'https://manhuavn.top')),
                max_pages=kwargs.get('max_pages', config_manager.get('max_pages', 10)),
                worker_count=kwargs.get('worker_count', config_manager.get('worker_count', 5))
            )
        else:
            logger.error(f"Không hỗ trợ loại crawler: {crawler_type}")
            raise ValueError(f"Không hỗ trợ loại crawler: {crawler_type}")