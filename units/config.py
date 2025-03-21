import json
import os
import logging

logger = logging.getLogger(__name__)

class ConfigManager:
    """Quản lý cấu hình ứng dụng"""
    
    def __init__(self, config_file="config.json"):
        """
        Khởi tạo ConfigManager
        
        Args:
            config_file: Đường dẫn đến file config
        """
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self):
        """
        Tải cấu hình từ file
        
        Returns:
            Dict chứa cấu hình
        """
        default_config = {
            "max_pages": 10,
            "batch_size": 32,
            "last_used_website": "TruyenQQ",
            "last_used_filter": {
                "genre": "Tất cả thể loại",
                "min_views": 0,
                "min_comments": 0
            },
            "export_path": "output/"
        }
        
        if not os.path.exists(self.config_file):
            self.save_config(default_config)
            return default_config
            
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            return config
            
        except Exception as e:
            logger.error(f"Lỗi khi tải cấu hình: {str(e)}")
            return default_config
    
    def save_config(self, config=None):
        """
        Lưu cấu hình vào file
        
        Args:
            config: Dict chứa cấu hình cần lưu
        """
        if config:
            self.config = config
            
        try:
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=4, ensure_ascii=False)
                
            logger.debug("Đã lưu cấu hình")
            
        except Exception as e:
            logger.error(f"Lỗi khi lưu cấu hình: {str(e)}")
    
    def get(self, key, default=None):
        """
        Lấy giá trị cấu hình theo key
        
        Args:
            key: Khóa cần lấy
            default: Giá trị mặc định nếu không tìm thấy
            
        Returns:
            Giá trị cấu hình
        """
        return self.config.get(key, default)
    
    def set(self, key, value):
        """
        Đặt giá trị cấu hình
        
        Args:
            key: Khóa cần đặt
            value: Giá trị mới
        """
        self.config[key] = value
        self.save_config()