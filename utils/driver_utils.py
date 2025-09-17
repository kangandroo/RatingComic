import time
import logging
import os
import gc
import signal
import psutil
import multiprocessing
from multiprocessing import current_process
from functools import wraps
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from utils.sqlite_helper import SQLiteHelper

logger = logging.getLogger(__name__)

# Thiết lập giới hạn tài nguyên
MAX_MEMORY_PERCENT = 80
MAX_DRIVER_INSTANCES = 25
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3

# Semaphore để kiểm soát số lượng driver
driver_semaphore = multiprocessing.Semaphore(MAX_DRIVER_INSTANCES)


def retry(max_retries=MAX_RETRIES, delay=2):
    """Decorator để thử lại các hàm nếu chúng thất bại"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"Hàm {func.__name__} thất bại sau {max_retries} lần thử: {e}")
                        raise
                    logger.warning(f"Thử lại {func.__name__} lần {retries}/{max_retries} sau {delay} giây. Lỗi: {e}")
                    time.sleep(delay * (2 ** (retries - 1)))  # Exponential backoff
            return None
        return wrapper
    return decorator


def check_system_resources():
    """Kiểm tra tài nguyên hệ thống và trả về True nếu đủ tài nguyên để tiếp tục"""
    try:
        mem = psutil.virtual_memory()
        if mem.percent > MAX_MEMORY_PERCENT:
            logger.warning(f"Cảnh báo: Sử dụng bộ nhớ cao ({mem.percent}%). Tạm dừng để giải phóng tài nguyên.")
            gc.collect()  # Thu gom rác
            time.sleep(5)  # Đợi hệ thống giải phóng tài nguyên
            return False
        return True
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra tài nguyên hệ thống: {e}")
        return True  # Cho phép tiếp tục nếu không thể kiểm tra


def setup_signal_handlers():
    """Thiết lập xử lý tín hiệu để đảm bảo tài nguyên được giải phóng"""
    if os.name != 'nt':  # Chỉ trên hệ thống không phải Windows
        def handle_sigterm(sig, frame):
            logger.info(f"Process {current_process().name} nhận tín hiệu SIGTERM. Đang dọn dẹp...")
            import sys
            sys.exit(0)
        signal.signal(signal.SIGTERM, handle_sigterm)


def init_process():
    """Khởi tạo các thiết lập cho mỗi process"""
    multiprocessing.current_process().daemon = False
    setup_signal_handlers()


def get_text_safe(element, selector, default="N/A"):
    """Lấy text an toàn từ phần tử"""
    try:
        # Hỗ trợ cả CSS selector string và By object
        if isinstance(selector, str):
            elements = element.find_elements(By.CSS_SELECTOR, selector)
        else:
            # Nếu selector là tuple (By.*, "selector_string")
            elements = element.find_elements(*selector)
        
        if not elements:
            return default
        text = elements[0].text.strip()
        return text if text else default
    except (NoSuchElementException, StaleElementReferenceException):
        return default
    except Exception:
        return default


def process_comic_worker(params, crawl_function, setup_driver_function):
    """
    Hàm để xử lý một truyện trong một process riêng biệt
    
    Args:
        params: Tuple chứa (comic, db_path, base_url, worker_id)
        crawl_function: Function để crawl chi tiết truyện
        setup_driver_function: Function để tạo driver
    """
    comic, db_path, base_url, worker_id = params
    
    driver = None
    sqlite_helper = None
    
    try:
        # Kiểm tra tài nguyên trước khi tạo driver
        if not check_system_resources():
            comic_name = comic.get('Tên truyện') or comic.get('ten_truyen', '')
            logger.warning(f"Worker {worker_id}: Tài nguyên hệ thống không đủ, bỏ qua truyện {comic_name}")
            return None
        
        # Mở kết nối database
        try:
            sqlite_helper = SQLiteHelper(db_path)
        except Exception as e:
            logger.error(f"Worker {worker_id}: Không thể kết nối đến database: {e}")
            return None
        
        # Giới hạn số lượng driver đồng thời
        with driver_semaphore:
            # Tạo driver mới cho mỗi process
            try:
                driver = setup_driver_function()
                logger.debug(f"Worker {worker_id}: Đã tạo driver thành công")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể tạo driver: {e}")
                return None
            
            # Xử lý truyện
            try:
                result = crawl_function(comic, driver, sqlite_helper, base_url, worker_id)
                return result
            except Exception as e:
                comic_name = comic.get('Tên truyện') or comic.get('ten_truyen', '')
                logger.error(f"Worker {worker_id}: Lỗi khi xử lý truyện {comic_name}: {e}")
                return None
                
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi không xác định: {e}")
        return None
    finally:
        # Đảm bảo giải phóng tài nguyên
        if driver:
            try:
                driver.quit()
                logger.debug(f"Worker {worker_id}: Đã đóng driver")
            except:
                pass
        if sqlite_helper:
            try:
                sqlite_helper.close_all_connections()
                logger.debug(f"Worker {worker_id}: Đã đóng kết nối database")
            except:
                pass