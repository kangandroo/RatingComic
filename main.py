import sys
import logging
import os
import multiprocessing
import subprocess
from pathlib import Path

# Đơn giản hóa xử lý multiprocessing
if hasattr(sys, 'frozen'):
    # Chỉ dùng freeze_support, không sửa đổi Process class
    multiprocessing.freeze_support()
    
    # Nếu trên Windows, đặt start method 
    if sys.platform.startswith('win'):
        if multiprocessing.get_start_method(allow_none=True) != 'spawn':
            multiprocessing.set_start_method('spawn', force=True)

from PyQt6.QtWidgets import QApplication
from ui.main_window import MainWindow
from utils.config_manager import ConfigManager
from crawlers.crawler_factory import CrawlerFactory

# Thiết lập logging
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Định dạng logging
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    # Thiết lập root logger
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(f"{log_dir}/rating_comic.log", encoding="utf-8"),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Thiết lập logger cho thư viện bên thứ ba
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

def cleanup_before_start():
    try:
        cleanup_script = Path(__file__).parent / "cleanup_processes.py"
        if cleanup_script.exists():
            subprocess.run([sys.executable, str(cleanup_script)], 
                          timeout=30, 
                          stderr=subprocess.PIPE, 
                          stdout=subprocess.PIPE)
            print("Đã hoàn thành quá trình dọn dẹp tiến trình")
    except Exception as e:
        print(f"Lỗi khi dọn dẹp tiến trình: {e}")

if __name__ == "__main__":
    cleanup_before_start()
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Khởi động ứng dụng Rating Comic")
    
    # Khởi tạo ứng dụng PyQt
    app = QApplication(sys.argv)
    
    # Tạo thư mục cần thiết
    os.makedirs("database", exist_ok=True)
    os.makedirs("output", exist_ok=True)
    
    try:
        # Khởi tạo ConfigManager
        config_manager = ConfigManager("config/config.json")
        
        # Khởi tạo CrawlerFactory
        CrawlerFactory.initialize(config_manager)
        
        # Khởi tạo và hiển thị MainWindow
        window = MainWindow(config_manager)
        window.show()
        
        # Chạy ứng dụng
        sys.exit(app.exec())
    except Exception as e:
        logger.error(f"Lỗi khi khởi động ứng dụng: {str(e)}")
        sys.exit(1)