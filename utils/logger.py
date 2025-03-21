import logging
import sys
import codecs
import os

def setup_logger():
    """
    Thiết lập logger cho toàn bộ ứng dụng với hỗ trợ Unicode
    """
    # Tạo logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Xóa tất cả handler cũ (nếu có)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Tạo file handler
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler('logs/app.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Định dạng log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    file_handler.setFormatter(formatter)
    
    # Thêm file handler vào logger
    logger.addHandler(file_handler)
    
    # Tạo handler cho stdout với UTF-8 encoding
    try:
        # Thử thiết lập stdout với UTF-8
        if sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')
        
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    except:
        # Nếu không thể thiết lập UTF-8, không hiển thị trong console
        pass
    
    return logger