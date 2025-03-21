import logging
import sys

def setup_logger():
    """
    Thiết lập logger cho toàn bộ ứng dụng
    
    Returns:
        Logger instance
    """
    # Tạo logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Xóa tất cả handler cũ (nếu có)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Tạo handler mới cho stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Định dạng log
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    console_handler.setFormatter(formatter)
    
    # Thêm handler vào logger
    logger.addHandler(console_handler)
    
    # Tạo file handler
    file_handler = logging.FileHandler('app.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Thêm file handler vào logger
    logger.addHandler(file_handler)
    
    return logger