import os
import json
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """
    Quản lý cấu hình của ứng dụng
    """
    
    def __init__(self, config_file="config/config.json"):
        """
        Khởi tạo ConfigManager
        
        Args:
            config_file: Đường dẫn đến file cấu hình
        """
        self.config_file = config_file
        self.config = {}
        
        # Tạo thư mục config nếu chưa tồn tại
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        
        # Tạo file config mặc định nếu chưa tồn tại
        if not os.path.exists(config_file):
            self._create_default_config()
        
        # Load cấu hình từ file
        self.load_config()
        
        logger.info(f"Đã khởi tạo ConfigManager với file: {config_file}")
    
    def _create_default_config(self):
        """Tạo file cấu hình mặc định"""
        default_config = {
            "chrome_driver_path": "",  # Để trống để Selenium tự tìm
            "max_pages": 10,  # Số trang tối đa để crawl
            "worker_count": 5,  # Số worker cho multi-threading
            "supported_websites": {
                "TruyenQQ": "https://truyenqqgo.com",
                "NetTruyen": "https://nettruyenvia.com",
                "Manhuavn": "https://manhuavn.top",
                "Truyentranh3q": "https://truyentranh3q.com"
            },
            "database_folder": "database",
            "sentiment_analysis": {
                "use_transformer": True,  # Sử dụng transformer model
                "model_name": "cardiffnlp/twitter-xlm-roberta-base-sentiment",  # Mô hình đa ngôn ngữ
                "cache_dir": "models"  # Thư mục cache cho mô hình
            }
        }
        
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, ensure_ascii=False, indent=4)
                
            logger.info(f"Đã tạo file cấu hình mặc định: {self.config_file}")
        except Exception as e:
            logger.error(f"Lỗi khi tạo file cấu hình mặc định: {str(e)}")
                
    def load_config(self):
        """Load cấu hình từ file"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
                
            logger.info("Đã load cấu hình thành công")
        except Exception as e:
            logger.error(f"Lỗi khi load cấu hình: {str(e)}")
            # Tạo cấu hình mặc định nếu có lỗi
            self._create_default_config()
            
            # Thử load lại
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            except:
                self.config = {}
    
    def save_config(self):
        """Lưu cấu hình vào file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=4)
                
            logger.info("Đã lưu cấu hình thành công")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi lưu cấu hình: {str(e)}")
            return False
    
    def get(self, key, default=None):
        """
        Lấy giá trị cấu hình theo key
        
        Args:
            key: Key của cấu hình
            default: Giá trị mặc định nếu không tìm thấy
            
        Returns:
            Giá trị cấu hình
        """
        return self.config.get(key, default)
    
    def set(self, key, value):
        """
        Thiết lập giá trị cấu hình
        
        Args:
            key: Key của cấu hình
            value: Giá trị cần thiết lập
            
        Returns:
            bool: True nếu thành công, False nếu thất bại
        """
        try:
            self.config[key] = value
            return self.save_config()
        except Exception as e:
            logger.error(f"Lỗi khi thiết lập cấu hình '{key}': {str(e)}")
            return False
    
    def get_chrome_driver_path(self):
        """
        Lấy đường dẫn đến ChromeDriver
        
        Returns:
            str: Đường dẫn đến ChromeDriver
        """
        return self.get("chrome_driver_path", "")
    
    def get_supported_websites(self):
        """
        Lấy danh sách website được hỗ trợ
        
        Returns:
            dict: Dictionary chứa tên và URL của các website
        """
        return self.get("supported_websites", {})
    
    def get_database_folder(self):
        """
        Lấy thư mục database
        
        Returns:
            str: Đường dẫn đến thư mục database
        """
        return self.get("database_folder", "database")