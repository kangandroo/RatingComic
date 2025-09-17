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
        params: Tuple (comic, db_path, website_type, progress_queue, worker_id, comic_url)
    
    Returns:
        tuple: (comic_url, comments, error)
    """
    if len(params) == 6:
        comic, db_path, website_type, progress_queue, worker_id, comic_url = params
    else:
        # Backward compatibility
        comic, db_path, website_type, progress_queue, worker_id = params
        comic_url = comic.get("link_truyen", "")
    
    driver = None
    sqlite_helper = None
    comments = []
    error = None
    
    try:
        logger.info(f"Worker {worker_id}: Bắt đầu crawl comment cho {comic.get('ten_truyen', 'Unknown')}")
        
        # Kiểm soát số driver đồng thời
        with driver_semaphore:
            # Khởi tạo database connection
            try:
                sqlite_helper = SQLiteHelper(db_path)
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể kết nối database: {e}")
                return (comic_url, [], str(e))
            
            # Tạo driver
            try:
                driver = create_comment_driver()
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể tạo driver: {e}")
                return (comic_url, [], str(e))
            
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
                    return (comic_url, [], f"Unsupported website: {website_type}")
                
                # Lưu comments vào database
                if comments and sqlite_helper:
                    save_comments_to_db(sqlite_helper, comic, comments, website_type)
                
                # Báo cáo tiến trình
                if progress_queue:
                    progress_queue.put({"type": "progress", "worker_id": worker_id, "count": len(comments)})
                
                logger.info(f"Worker {worker_id}: Hoàn thành crawl {len(comments)} comments cho {comic.get('ten_truyen', '')}")
                return (comic_url, comments, None)
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Lỗi khi crawl comments: {e}")
                logger.error(traceback.format_exc())
                return (comic_url, [], str(e))
    
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi tổng quát: {e}")
        return (comic_url, [], str(e))
    
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
            comics_list: Danh sách truyện cần crawl comments hoặc crawl_data
            progress_callback: Callback để báo cáo tiến trình
            
        Returns:
            dict: {comic_url: [comments]} - Comments theo từng truyện
        """
        if not comics_list:
            return {}
        
        total_comics = len(comics_list)
        comments_by_url = {}
        errors = []
        
        try:
            # Thiết lập multiprocessing context
            if not hasattr(multiprocessing, 'get_start_method') or multiprocessing.get_start_method() != 'spawn':
                multiprocessing.set_start_method('spawn', force=True)
            
            # Tạo Manager để chia sẻ progress queue
            with Manager() as manager:
                progress_queue = manager.Queue()
                
                # Chuẩn bị parameters cho các worker
                worker_params = []
                for i, comic_data in enumerate(comics_list):
                    # Xử lý cả format cũ (comic dict) và format mới (crawl_data)
                    if isinstance(comic_data, dict) and 'comic_url' in comic_data:
                        # Format mới từ crawl_comments_batch
                        comic_url = comic_data['comic_url']
                        comic_name = comic_data.get('comic_name', 'Unknown')
                        source = comic_data.get('source', self.website_type)
                        original_comic = comic_data.get('comic_data', comic_data)
                    else:
                        # Format cũ - comic dict trực tiếp
                        comic_url = comic_data.get("link_truyen", "")
                        comic_name = comic_data.get('ten_truyen', 'Unknown')
                        source = comic_data.get('nguon', self.website_type)
                        original_comic = comic_data
                    
                    if comic_url:
                        worker_params.append((
                            original_comic, self.db_path, source, progress_queue, i, comic_url
                        ))
                
                if not worker_params:
                    logger.warning("Không có truyện hợp lệ để crawl comments")
                    return {}
                
                # Sử dụng multiprocessing Pool
                with Pool(processes=self.max_workers) as pool:
                    logger.info(f"Bắt đầu crawl comments cho {len(worker_params)} truyện với {self.max_workers} processes")
                    
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
                                    progress_callback(int((completed / total_comics) * 100))
                        except:
                            time.sleep(0.1)
                            continue
                    
                    # Lấy kết quả
                    results = result.get(timeout=300)  # 5 phút timeout
                    
                    # Xử lý kết quả
                    for i, (comic_url, comments, error) in enumerate(results):
                        if error:
                            errors.append(f"Comic {i+1}: {error}")
                            comments_by_url[comic_url] = []
                        else:
                            comments_by_url[comic_url] = comments if comments else []
                    
                    total_comments = sum(len(comments) for comments in comments_by_url.values())
                    logger.info(f"Hoàn thành crawl comments: {total_comments} comments cho {len(worker_params)} truyện")
                    
                    if errors:
                        logger.warning(f"Có {len(errors)} lỗi trong quá trình crawl: {errors[:3]}")
                    
                    return comments_by_url
                        
        except Exception as e:
            logger.error(f"Lỗi nghiêm trọng trong crawl_comments_parallel: {str(e)}")
            logger.error(traceback.format_exc())
            # Trả về dict rỗng cho tất cả comics
            empty_result = {}
            for comic_data in comics_list:
                if isinstance(comic_data, dict) and 'comic_url' in comic_data:
                    comic_url = comic_data['comic_url']
                else:
                    comic_url = comic_data.get("link_truyen", "")
                if comic_url:
                    empty_result[comic_url] = []
            return empty_result


# Thiết lập exception handler
def global_exception_handler(exctype, value, tb):
    logger.critical("Lỗi không bắt được trong comment crawler: %s", 
                    ''.join(traceback.format_exception(exctype, value, tb)))

sys.excepthook = global_exception_handler
