"""
Comment Crawler với Multiprocessing + Multithreading
Hỗ trợ crawl comment song song cho nhiều truyện
"""

import logging
import time
import random
import gc
import os
import sys
import multiprocessing
from multiprocessing import Pool, Manager, Value, Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from datetime import datetime, timedelta
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from utils.sqlite_helper import SQLiteHelper

logger = logging.getLogger(__name__)

# Thiết lập giới hạn tài nguyên
MAX_PROCESSES = min(4, multiprocessing.cpu_count())  # Tối đa 4 processes
MAX_THREADS_PER_PROCESS = 3  # Mỗi process có tối đa 3 threads
MAX_DRIVER_INSTANCES = 12  # Tổng số driver toàn cục
DEFAULT_TIMEOUT = 15  # Timeout ngắn hơn cho comment crawling

# Semaphore toàn cục để kiểm soát số driver
driver_semaphore = multiprocessing.Semaphore(MAX_DRIVER_INSTANCES)

def create_comment_driver():
    """Tạo Chrome driver tối ưu cho crawl comment"""
    chrome_options = Options()
    
    # Chạy headless và tắt hình ảnh để tăng tốc
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")  # Tắt JS nếu không cần thiết
    chrome_options.add_argument("--disable-css")  # Tắt CSS để tăng tốc
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--memory-model=low")
    
    # Thiết lập không tải hình ảnh và media
    prefs = {
        'profile.default_content_settings.images': 2,
        'profile.managed_default_content_settings.images': 2,
        'profile.default_content_settings.media_stream': 2,
        'profile.default_content_settings.stylesheets': 2,
    }
    chrome_options.add_experimental_option('prefs', prefs)
    
    # Tắt logging
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    try:
        service = Service(log_path=os.devnull)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(DEFAULT_TIMEOUT)
        driver.set_script_timeout(DEFAULT_TIMEOUT)
        return driver
    except Exception as e:
        logger.error(f"Lỗi khi tạo comment driver: {e}")
        raise


def safe_extract_text(element, selector, default=""):
    """Trích xuất text an toàn từ element"""
    try:
        if selector:
            elements = element.find_elements(By.CSS_SELECTOR, selector)
            if elements:
                return elements[0].text.strip()
        else:
            return element.text.strip()
        return default
    except Exception:
        return default


def crawl_single_comic_comments(params):
    """
    Crawl comment cho một truyện trong một process riêng biệt
    
    Args:
        params: Tuple (comic, db_path, website_type, progress_queue, worker_id)
    
    Returns:
        dict: Kết quả crawl cho comic này
    """
    comic, db_path, website_type, progress_queue, worker_id = params
    
    driver = None
    sqlite_helper = None
    comments = []
    
    try:
        logger.info(f"Worker {worker_id}: Bắt đầu crawl comment cho {comic.get('ten_truyen', 'Unknown')}")
        
        # Kiểm soát số driver đồng thời
        with driver_semaphore:
            # Khởi tạo database connection
            try:
                sqlite_helper = SQLiteHelper(db_path)
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể kết nối database: {e}")
                return {"comic": comic, "comments": [], "error": str(e)}
            
            # Tạo driver
            try:
                driver = create_comment_driver()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể tạo driver: {e}")
                return {"comic": comic, "comments": [], "error": str(e)}
            
            # Crawl comments dựa trên website type
            try:
                if website_type == "TruyenQQ":
                    comments = crawl_truyenqq_comments(driver, comic, worker_id)
                elif website_type == "NetTruyen":
                    comments = crawl_nettruyen_comments(driver, comic, worker_id)
                elif website_type == "Manhuavn":
                    comments = crawl_manhuavn_comments(driver, comic, worker_id)
                elif website_type == "Truyentranh3q":
                    comments = crawl_truyentranh3q_comments(driver, comic, worker_id)
                else:
                    logger.warning(f"Worker {worker_id}: Không hỗ trợ website type: {website_type}")
                    return {"comic": comic, "comments": [], "error": f"Unsupported website: {website_type}"}
                
                # Lưu comments vào database
                if comments and sqlite_helper:
                    save_comments_to_db(sqlite_helper, comic, comments, website_type)
                
                # Báo cáo tiến trình
                if progress_queue:
                    progress_queue.put({"type": "progress", "worker_id": worker_id, "count": len(comments)})
                
                logger.info(f"Worker {worker_id}: Hoàn thành crawl {len(comments)} comments cho {comic.get('ten_truyen', '')}")
                return {"comic": comic, "comments": comments, "error": None}
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Lỗi khi crawl comments: {e}")
                logger.error(traceback.format_exc())
                return {"comic": comic, "comments": [], "error": str(e)}
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi tổng quát: {e}")
        return {"comic": comic, "comments": [], "error": str(e)}
    
    finally:
        # Dọn dẹp tài nguyên
        if driver:
            try:
                driver.quit()
            except:
                pass
        if sqlite_helper:
            try:
                sqlite_helper.close_all_connections()
            except:
                pass
        # Gọi garbage collector
        gc.collect()


def crawl_truyenqq_comments(driver, comic, worker_id):
    """Crawl comments cho TruyenQQ"""
    comments = []
    
    try:
        url = comic.get("link_truyen", "")
        if not url:
            return comments
        
        driver.get(url)
        
        # Đợi trang tải
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Tìm phần comment (cần điều chỉnh selector theo cấu trúc thực tế)
        comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment, .comment-item, .fb-comment")
        
        for element in comment_elements:
            try:
                comment_text = safe_extract_text(element, ".comment-content, .comment-text", "")
                author = safe_extract_text(element, ".comment-author, .author-name", "Anonymous")
                time_str = safe_extract_text(element, ".comment-time, .time", "")
                
                if comment_text.strip():
                    comments.append({
                        "content": comment_text,
                        "author": author,
                        "time": time_str,
                        "comic_url": url
                    })
            except Exception as e:
                logger.debug(f"Worker {worker_id}: Lỗi khi parse comment: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi khi crawl TruyenQQ comments: {e}")
    
    return comments


def crawl_nettruyen_comments(driver, comic, worker_id):
    """Crawl comments cho NetTruyen"""
    comments = []
    
    try:
        url = comic.get("link_truyen", "")
        if not url:
            return comments
        
        driver.get(url)
        
        # Đợi trang tải
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Tìm phần comment (điều chỉnh selector theo NetTruyen)
        comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment-list .comment, .comments .comment-item")
        
        for element in comment_elements:
            try:
                comment_text = safe_extract_text(element, ".comment-body, .comment-content", "")
                author = safe_extract_text(element, ".comment-author", "Anonymous")
                time_str = safe_extract_text(element, ".comment-date", "")
                
                if comment_text.strip():
                    comments.append({
                        "content": comment_text,
                        "author": author,
                        "time": time_str,
                        "comic_url": url
                    })
            except Exception as e:
                logger.debug(f"Worker {worker_id}: Lỗi khi parse comment: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi khi crawl NetTruyen comments: {e}")
    
    return comments


def crawl_manhuavn_comments(driver, comic, worker_id):
    """Crawl comments cho Manhuavn"""
    comments = []
    
    try:
        url = comic.get("link_truyen", "")
        if not url:
            return comments
        
        driver.get(url)
        
        # Đợi trang tải
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Tìm phần comment (điều chỉnh selector theo Manhuavn)
        comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment, .review-item")
        
        for element in comment_elements:
            try:
                comment_text = safe_extract_text(element, ".comment-text, .review-content", "")
                author = safe_extract_text(element, ".comment-user, .reviewer", "Anonymous")
                time_str = safe_extract_text(element, ".comment-time", "")
                
                if comment_text.strip():
                    comments.append({
                        "content": comment_text,
                        "author": author,
                        "time": time_str,
                        "comic_url": url
                    })
            except Exception as e:
                logger.debug(f"Worker {worker_id}: Lỗi khi parse comment: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi khi crawl Manhuavn comments: {e}")
    
    return comments


def crawl_truyentranh3q_comments(driver, comic, worker_id):
    """Crawl comments cho Truyentranh3q"""
    comments = []
    
    try:
        url = comic.get("link_truyen", "")
        if not url:
            return comments
        
        driver.get(url)
        
        # Đợi trang tải
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Tìm phần comment (điều chỉnh selector theo Truyentranh3q)
        comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment-item, .user-comment")
        
        for element in comment_elements:
            try:
                comment_text = safe_extract_text(element, ".comment-content", "")
                author = safe_extract_text(element, ".comment-author", "Anonymous")
                time_str = safe_extract_text(element, ".comment-time", "")
                
                if comment_text.strip():
                    comments.append({
                        "content": comment_text,
                        "author": author,
                        "time": time_str,
                        "comic_url": url
                    })
            except Exception as e:
                logger.debug(f"Worker {worker_id}: Lỗi khi parse comment: {e}")
                continue
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi khi crawl Truyentranh3q comments: {e}")
    
    return comments


def save_comments_to_db(sqlite_helper, comic, comments, website_type):
    """Lưu comments vào database"""
    try:
        table_name = f"{website_type.lower()}_comments"
        
        for comment in comments:
            sqlite_helper.insert_data(table_name, {
                "comic_id": comic.get("id", ""),
                "comic_name": comic.get("ten_truyen", ""),
                "comic_url": comment.get("comic_url", ""),
                "comment_content": comment.get("content", ""),
                "comment_author": comment.get("author", ""),
                "comment_time": comment.get("time", ""),
                "crawl_time": datetime.now().isoformat()
            })
        
        logger.info(f"Đã lưu {len(comments)} comments vào database")
        
    except Exception as e:
        logger.error(f"Lỗi khi lưu comments vào database: {e}")


class CommentCrawler:
    """
    Class chính để quản lý crawl comments với multiprocessing + multithreading
    """
    
    def __init__(self, website_type, db_path, max_workers=None):
        """
        Khởi tạo CommentCrawler
        
        Args:
            website_type: Loại website (TruyenQQ, NetTruyen, Manhuavn, Truyentranh3q)
            db_path: Đường dẫn database
            max_workers: Số worker tối đa (None = auto)
        """
        self.website_type = website_type
        self.db_path = db_path
        self.max_workers = max_workers or min(MAX_PROCESSES, multiprocessing.cpu_count())
        
        logger.info(f"Khởi tạo CommentCrawler cho {website_type} với {self.max_workers} workers")
    
    def crawl_comments_parallel(self, comics_list, progress_callback=None):
        """
        Crawl comments cho danh sách truyện sử dụng multiprocessing
        
        Args:
            comics_list: Danh sách truyện cần crawl comments
            progress_callback: Callback để báo cáo tiến trình
            
        Returns:
            dict: Kết quả crawl {total_comments, total_comics, errors}
        """
        if not comics_list:
            return {"total_comments": 0, "total_comics": 0, "errors": []}
        
        total_comics = len(comics_list)
        total_comments = 0
        errors = []
        
        try:
            # Thiết lập multiprocessing context
            if not hasattr(multiprocessing, 'get_start_method') or multiprocessing.get_start_method() != 'spawn':
                multiprocessing.set_start_method('spawn', force=True)
            
            # Tạo Manager để chia sẻ progress queue
            with Manager() as manager:
                progress_queue = manager.Queue()
                
                # Chuẩn bị parameters cho các worker
                worker_params = [
                    (comic, self.db_path, self.website_type, progress_queue, i)
                    for i, comic in enumerate(comics_list)
                ]
                
                # Sử dụng multiprocessing Pool
                with Pool(processes=self.max_workers) as pool:
                    logger.info(f"Bắt đầu crawl comments cho {total_comics} truyện với {self.max_workers} processes")
                    
                    # Map async để có thể theo dõi progress
                    result = pool.map_async(crawl_single_comic_comments, worker_params)
                    
                    # Theo dõi tiến trình
                    completed = 0
                    while not result.ready():
                        try:
                            # Đọc progress từ queue (non-blocking)
                            progress_data = progress_queue.get_nowait()
                            if progress_data["type"] == "progress":
                                completed += 1
                                if progress_callback:
                                    progress_callback.emit(int((completed / total_comics) * 100))
                        except:
                            pass
                        time.sleep(0.1)
                    
                    # Lấy kết quả
                    results = result.get(timeout=300)  # Timeout 5 phút
                    
                    # Xử lý kết quả
                    for result_data in results:
                        if result_data.get("error"):
                            errors.append({
                                "comic": result_data["comic"].get("ten_truyen", "Unknown"),
                                "error": result_data["error"]
                            })
                        else:
                            total_comments += len(result_data.get("comments", []))
            
            logger.info(f"Hoàn thành crawl comments: {total_comments} comments từ {total_comics} truyện")
            
            return {
                "total_comments": total_comments,
                "total_comics": total_comics,
                "errors": errors
            }
        
        except Exception as e:
            logger.error(f"Lỗi khi crawl comments parallel: {e}")
            logger.error(traceback.format_exc())
            return {
                "total_comments": 0,
                "total_comics": 0,
                "errors": [{"comic": "System", "error": str(e)}]
            }


# Thiết lập exception handler
def global_exception_handler(exctype, value, tb):
    logger.critical("Lỗi không bắt được trong comment crawler: %s", 
                    ''.join(traceback.format_exception(exctype, value, tb)))

sys.excepthook = global_exception_handler
