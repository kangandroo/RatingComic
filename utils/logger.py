import os
import logging
from datetime import datetime

# Tạo thư mục logs nếu chưa tồn tại
log_dir = 'logs'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Tạo định dạng logger chung
date_format = '%Y-%m-%d %H:%M:%S'
log_format = '%(asctime)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(log_format, date_format)

# Tạo file handler với tên file dựa trên ngày
log_filename = os.path.join(log_dir, f"comic_analyzer_{datetime.now().strftime('%Y%m%d')}.log")
file_handler = logging.FileHandler(log_filename)
file_handler.setFormatter(formatter)

# Tạo console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# Tạo default logger
default_logger = logging.getLogger('comic_analyzer')
default_logger.setLevel(logging.INFO)
default_logger.addHandler(file_handler)
default_logger.addHandler(console_handler)

# Tạo logger cho crawler
crawler_logger = logging.getLogger('crawler')
crawler_logger.setLevel(logging.INFO)
crawler_logger.addHandler(file_handler)
crawler_logger.addHandler(console_handler)

# Tạo logger cho analyzer
analyzer_logger = logging.getLogger('analyzer')
analyzer_logger.setLevel(logging.INFO)
analyzer_logger.addHandler(file_handler)
analyzer_logger.addHandler(console_handler)

# Tạo logger cho database operations
db_logger = logging.getLogger('database')
db_logger.setLevel(logging.INFO)
db_logger.addHandler(file_handler)
db_logger.addHandler(console_handler)