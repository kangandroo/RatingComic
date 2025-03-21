from crawlers.truyenqq_crawler import TruyenQQCrawler
import logging

logger = logging.getLogger(__name__)

class CrawlerFactory:
    """Factory pattern để tạo crawler phù hợp"""
    
    @staticmethod
    def create_crawler(website_name, db_manager, **kwargs):
        """
        Tạo crawler dựa trên tên website
        
        Args:
            website_name: Tên website cần crawl
            db_manager: Database manager để lưu dữ liệu
            **kwargs: Các tham số bổ sung cho crawler
            
        Returns:
            Crawler tương ứng với website
        """
        if website_name == "TruyenQQ":
            return TruyenQQCrawler(db_manager, **kwargs)
        elif website_name == "NetTruyen":
            # TODO: Implement NetTruyen crawler
            raise NotImplementedError("NetTruyen crawler chưa được triển khai")
        elif website_name == "TruyenFull":
            # TODO: Implement TruyenFull crawler
            raise NotImplementedError("TruyenFull crawler chưa được triển khai")
        else:
            logger.error(f"Website không được hỗ trợ: {website_name}")
            raise ValueError(f"Website không được hỗ trợ: {website_name}")