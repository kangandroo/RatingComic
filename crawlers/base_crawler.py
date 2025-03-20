import time
import random
import re
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

class BaseCrawler:
    """Lớp cơ sở cho tất cả crawlers"""
    
    def __init__(self, logger=None, chromedriver_path=None):
        self.logger = logger or logging.getLogger(__name__)
        self.chromedriver_path = chromedriver_path
        self.name = "Base"
        self.base_url = ""
        
    def log(self, message, level="info", emit_log=None):
        """Ghi log thông tin"""
        # Gửi log qua socket nếu có
        if emit_log:
            emit_log(message)
            
        # Ghi log thông thường
        if level == "info":
            self.logger.info(message)
        elif level == "error":
            self.logger.error(message)
        elif level == "warning":
            self.logger.warning(message)
        
    def setup_driver(self):
        """Khởi tạo và trả về WebDriver đã cấu hình"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--lang=vi")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
        
        if self.chromedriver_path:
            from selenium.webdriver.chrome.service import Service
            return webdriver.Chrome(service=Service(self.chromedriver_path), options=options)
        else:
            return webdriver.Chrome(options=options)
    
    def get_text_safe(self, element, selector):
        """Lấy text an toàn từ một element"""
        try:
            return element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except Exception:
            return "N/A"
    
    def extract_number(self, text_value):
        """Trích xuất số từ chuỗi text có nhiều định dạng"""
        try:
            # Xử lý trường hợp đã là số
            if isinstance(text_value, (int, float)):
                return int(text_value)
                
            # Xử lý giá trị trống hoặc không có
            if not text_value or text_value == 'N/A':
                return 0
                
            text_value = str(text_value).strip()
            
            # Xử lý hậu tố K (nghìn) và M (triệu)
            if 'K' in text_value.upper():
                num_part = text_value.upper().replace('K', '')
                # Làm sạch và chuyển đổi
                if num_part.count('.') == 1:
                    return int(float(num_part) * 1000)
                else:
                    cleaned = num_part.replace('.', '').replace(',', '')
                    return int(float(cleaned) * 1000)
                
            elif 'M' in text_value.upper():
                num_part = text_value.upper().replace('M', '')
                if num_part.count('.') == 1:
                    return int(float(num_part) * 1000000)
                else:
                    cleaned = num_part.replace('.', '').replace(',', '')
                    return int(float(cleaned) * 1000000)
            else:
                # Xử lý số có nhiều dấu chấm hoặc phẩy
                if text_value.count('.') > 1:
                    text_value = text_value.replace('.', '')
                
                text_value = text_value.replace(',', '')
                
                return int(float(text_value))
        except Exception as e:
            self.logger.error(f"Lỗi khi trích xuất số từ '{text_value}': {e}")
            return 0
    
    def extract_chapter_number(self, chapter_text):
        """Trích xuất số chương từ text"""
        try:
            # Tìm số trong text
            match = re.search(r'Chapter\s+(\d+)', chapter_text)
            if match:
                return int(match.group(1))
            
            # Thử với định dạng "Chương X"
            match = re.search(r'Chương\s+(\d+)', chapter_text)
            if match:
                return int(match.group(1))
                
            # Tìm bất kỳ số nào trong chuỗi
            match = re.search(r'(\d+)', chapter_text)
            if match:
                return int(match.group(1))
        except Exception:
            pass
        
        return 0  # Trả về 0 nếu không tìm thấy số chương
    
    def get_all_stories(self, num_pages=3, emit_log=None):
        """
        Lấy danh sách truyện từ nhiều trang
        
        Args:
            num_pages: Số trang cần crawl
            emit_log: Hàm callback để gửi log (cho WebSocket)
            
        Returns:
            List: Danh sách truyện
        """
        self.log("Method not implemented in base class", level="warning", emit_log=emit_log)
        return []
    
    def get_story_details(self, story, emit_log=None):
        """
        Lấy thông tin chi tiết của một truyện
        
        Args:
            story: Dict chứa thông tin cơ bản của truyện
            emit_log: Hàm callback để gửi log (cho WebSocket)
            
        Returns:
            Dict: Thông tin chi tiết truyện
        """
        self.log("Method not implemented in base class", level="warning", emit_log=emit_log)
        return story
    
    def get_comments(self, story, emit_log=None):
        """
        Lấy bình luận của một truyện
        
        Args:
            story: Dict chứa thông tin của truyện
            emit_log: Hàm callback để gửi log (cho WebSocket)
            
        Returns:
            List: Danh sách bình luận
        """
        self.log("Method not implemented in base class", level="warning", emit_log=emit_log)
        return []