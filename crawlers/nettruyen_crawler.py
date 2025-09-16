import time
import random
import logging
import os
import re
import gc
import signal
import psutil
import multiprocessing
from multiprocessing import Pool, Value, current_process
from functools import wraps
from datetime import datetime, timedelta
from seleniumbase import Driver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, StaleElementReferenceException
from crawlers.base_crawler import BaseCrawler
from utils.sqlite_helper import SQLiteHelper
from tempfile import mkdtemp

logger = logging.getLogger(__name__)

# Thiết lập giới hạn tài nguyên
MAX_MEMORY_PERCENT = 80  
MAX_DRIVER_INSTANCES = 25  
DEFAULT_TIMEOUT = 30  
MAX_RETRIES = 3  

# Semaphore để kiểm soát số lượng driver
driver_semaphore = multiprocessing.Semaphore(MAX_DRIVER_INSTANCES)

# Decorator để thêm retry mechanism
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

# Hàm kiểm tra RAM và tài nguyên hệ thống
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

# Signal handler để xử lý khi process bị kill
def setup_signal_handlers():
    """Thiết lập xử lý tín hiệu để đảm bảo tài nguyên được giải phóng"""
    if os.name != 'nt':  # Chỉ trên hệ thống không phải Windows
        def handle_sigterm(sig, frame):
            logger.info(f"Process {current_process().name} nhận tín hiệu SIGTERM. Đang dọn dẹp...")
            sys.exit(0)
        signal.signal(signal.SIGTERM, handle_sigterm)

# Hàm khởi tạo riêng cho mỗi process
def init_process():
    """Khởi tạo các thiết lập cho mỗi process"""
    multiprocessing.current_process().daemon = False
    setup_signal_handlers()

# Định nghĩa hàm xử lý truyện ở cấp độ module
def process_comic_worker(params):
    """Hàm để xử lý một truyện trong một process riêng biệt"""
    comic, db_path, base_url, worker_id = params
    
    time.sleep(random.uniform(1, 3)*(worker_id % 5 + 1) / 5)

    driver = None
    sqlite_helper = None
    
    try:
        # Kiểm tra tài nguyên trước khi tạo driver
        if not check_system_resources():
            logger.warning(f"Worker {worker_id}: Tài nguyên hệ thống không đủ, bỏ qua truyện {comic.get('Tên truyện', '')}")
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
                driver = setup_driver()
                logger.debug(f"Worker {worker_id}: Đã tạo driver thành công")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể tạo driver: {e}")
                return None
            
            # # Bypass Cloudflare
            # try:
            #     bypass_cloudflare(driver, base_url)
            # except Exception as e:
            #     logger.error(f"Worker {worker_id}: Không thể bypass Cloudflare: {e}")
            #     return None
                
            # Lấy chi tiết truyện
            try:
                detailed_comic = get_story_details(comic, driver, worker_id)
                
                if detailed_comic:
                    # Chuyển đổi định dạng
                    db_comic = transform_comic_data(detailed_comic)
                    
                    # Lưu vào database
                    try:
                        sqlite_helper.save_comic_to_db(db_comic, "NetTruyen")
                        logger.info(f"Worker {worker_id}: Hoàn thành lưu truyện {comic.get('Tên truyện', '')}")
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Lỗi khi lưu vào database: {e}")
                    
                    return detailed_comic
                    
            except Exception as e:
                logger.error(f"Worker {worker_id}: Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
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

def setup_driver():
    """Tạo và cấu hình SeleniumBase Driver để bypass Cloudflare"""
    try:
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
        os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
        os.environ['TF_USE_LEGACY_CPU'] = '0'
        os.environ['TF_DISABLE_MKL'] = '1'
        os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning,ignore::UserWarning'
        
        from seleniumbase import Driver
        
        driver = Driver(
            browser="chrome",   
            uc=True,           
            headless=True,      
            no_sandbox=True
        )
        
        # Thiết lập các timeout sau khi tạo driver
        driver.implicitly_wait(5)
        driver.set_page_load_timeout(DEFAULT_TIMEOUT)
        driver.set_script_timeout(DEFAULT_TIMEOUT)
        
        return driver
        
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo SeleniumBase driver: {e}")
        try:
            # Fallback với ít tùy chọn hơn
            return Driver(browser="chrome", headless=True, no_sandbox=True)
        except Exception as e2:
            logger.critical(f"Lỗi nghiêm trọng khi khởi tạo SeleniumBase driver: {e2}")
            raise RuntimeError(f"Không thể khởi tạo SeleniumBase driver: {e2}")

def get_text_safe(element, selector, default="N/A"):
    """Lấy text an toàn từ phần tử"""
    try:
        elements = element.find_elements("css selector", selector)
        if not elements:
            return default
        text = elements[0].text.strip()
        return text if text else default
    except (NoSuchElementException, StaleElementReferenceException):
        return default
    except Exception:
        return default

def extract_chapter_number(chapter_text):
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
            
        # Thử tìm bất kỳ số nào trong chuỗi
        match = re.search(r'(\d+)', chapter_text)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    
    return 0  # Trả về 0 nếu không tìm thấy số chương

def parse_number(text):
    """Chuyển đổi các số có đơn vị K, M và dấu phẩy thành số nguyên."""
    if text is None:
        return 0

    try:
        text = str(text).lower().strip().replace(",", ".")  # Ép kiểu và xử lý

        if text == "n/a":
            return 0

        multipliers = {"k": 1_000, "m": 1_000_000}
        multiplier = 1

        for suffix, value in multipliers.items():
            if suffix in text:
                multiplier = value
                text = text.replace(suffix, "")
                break

        cleaned_text = re.sub(r'[^\d.]', '', text)
        if not cleaned_text:
            return 0

        return int(float(cleaned_text) * multiplier)
    except (ValueError, TypeError):
        return 0

def extract_number(text_value):
    """
    Trích xuất số từ chuỗi với nhiều định dạng
    Ví dụ: '1,234' -> 1234, '5K' -> 5000, '3.2M' -> 3200000
    """
    if not text_value or text_value == "N/A":
        return 0
        
    # Chỉ lấy phần số từ chuỗi
    try:
        text_value = str(text_value).lower().strip()
        
        if "k" in text_value:
            num_part = text_value.replace('k', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                return int(float(num_part) * 1000)
            else:
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000)
            
        elif "m" in text_value:
            num_part = text_value.replace('m', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                return int(float(num_part) * 1000000)
            else:
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000000)
        else:
            # Lấy tất cả các số từ chuỗi
            numbers = re.findall(r'\d+', text_value)
            if numbers:
                # Lấy số lớn nhất nếu có nhiều số
                return max(map(int, numbers))
            return 0
    except Exception:
        return 0

def transform_comic_data(raw_comic):
    """Chuyển đổi dữ liệu raw sang định dạng database"""
    return {
        "ten_truyen": raw_comic.get("Tên truyện", ""),
        "tac_gia": raw_comic.get("Tác giả", "N/A"),
        "link_truyen": raw_comic.get("Link truyện", ""),
        "so_chuong": raw_comic.get("Số chương", 0),
        "luot_xem": parse_number(raw_comic.get("Lượt xem", 0)),
        "luot_theo_doi": parse_number(raw_comic.get("Lượt theo dõi", 0)),
        "rating": raw_comic.get("Đánh giá", ""),
        "luot_danh_gia": parse_number(raw_comic.get("Lượt đánh giá", 0)),
        "so_binh_luan": parse_number(raw_comic.get("Số bình luận", 0)),
        "trang_thai": raw_comic.get("Trạng thái", ""),
        "nguon": "NetTruyen",
    }

@retry(max_retries=2)
def bypass_cloudflare(driver, base_url):
    """Bypass Cloudflare protection"""
    try:
        url = f"{base_url}/?page={1}"
        logger.info(f"Đang truy cập {url} để bypass Cloudflare...")
        
        for attempt in range(3):
            try:
                driver.get(url)
                break
            except Exception as e:
                logger.warning(f"Lỗi khi truy cập URL để bypass Cloudflare, thử lần {attempt + 1}/3: {e}")
                time.sleep(random.uniform(2, 4))
        else:
            logger.error("Không thể truy cập URL để bypass Cloudflare sau 3 lần thử")
            return False
        
        # Đợi để Cloudflare hoàn tất kiểm tra
        logger.info("Đợi để vượt qua Cloudflare...")
        time.sleep(5)  # Thời gian đợi có thể điều chỉnh
        
        # Kiểm tra xem đã bypass thành công chưa
        if "Just a moment" in driver.page_source or "Checking your browser" in driver.page_source:
            logger.info("Đang chờ Cloudflare hoàn tất kiểm tra...")
            time.sleep(10)
            
        # Kiểm tra lại
        if "Just a moment" in driver.page_source or "Checking your browser" in driver.page_source:
            logger.warning("Cloudflare vẫn đang kiểm tra, chờ thêm...")
            time.sleep(15)
            
        logger.info("Đã vượt qua Cloudflare")
        return True
    except Exception as e:
        logger.error(f"Lỗi khi bypass Cloudflare: {e}")
        return False

@retry(max_retries=2)
def get_story_details(story, driver, worker_id=0):
    """Lấy thông tin chi tiết của truyện sử dụng driver được cung cấp"""
    try:
        for attempt in range(5):
            try:
                driver.get(story["Link truyện"])
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "li.author.row p.col-xs-8"))
                )
                break
            except Exception as e:
                logger.warning(f"Worker {worker_id}: Thử lần {attempt + 1} truy cập {story['Link truyện']}: {e}")
                time.sleep(random.uniform(2, 4))
        else:
            logger.error(f"Worker {worker_id}: Không thể truy cập trang sau 5 lần thử")
            return None

        # Lấy thông tin cơ bản với xử lý ngoại lệ chi tiết
        try:
            story["Tác giả"] = get_text_safe(driver, "li.author.row p.col-xs-8")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy tác giả: {e}")
            story["Tác giả"] = "N/A"
            
        try:    
            story["Trạng thái"] = get_text_safe(driver, "li.status.row p.col-xs-8")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy trạng thái: {e}")
            story["Trạng thái"] = "N/A"
            
        try:
            story["Đánh giá"] = get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(1)")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy đánh giá: {e}")
            story["Đánh giá"] = "0"
        
        # Lấy số liệu - xử lý an toàn với số liệu
        try:
            follow_text = get_text_safe(driver, ".follow span b.number_follow")
            story["Lượt theo dõi"] = follow_text
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt theo dõi: {e}")
            story["Lượt theo dõi"] = "0"
        
        try:
            view_text = get_text_safe(driver, "ul.list-info li:last-child p.col-xs-8")
            story["Lượt xem"] = view_text
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt xem: {e}")
            story["Lượt xem"] = "0"
        
        try:
            rating_count_text = get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(3)")
            story["Lượt đánh giá"] = rating_count_text
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt đánh giá: {e}")
            story["Lượt đánh giá"] = "0"

        # Cố gắng lấy số chương chính xác
        try:
            # Thử tìm chương mới nhất
            chapter_element = driver.find_element("css selector", ".list-chapter li:first-child a")
            if chapter_element:
                chapter_text = chapter_element.get_attribute("title")
                if chapter_text:
                    chapter_count = extract_chapter_number(chapter_text)
                    if chapter_count > 0:
                        story["Số chương"] = chapter_count
                
            # Nếu không tìm thấy số chương hoặc số chương = 0, thử đếm các chương
            if story.get("Số chương", 0) == 0:
                chapter_items = driver.find_elements("css selector", ".list-chapter li")
                story["Số chương"] = len(chapter_items)
        except Exception as e:
            logger.error(f"Worker {worker_id}: Lỗi khi lấy số chương: {e}")
            story["Số chương"] = 0

        # Lấy số bình luận
        try:
            comment_count_text = get_text_safe(driver, ".comment-count")
            story["Số bình luận"] = comment_count_text
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy số bình luận: {e}")
            story["Số bình luận"] = 0

    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi khi lấy thông tin chi tiết truyện {story.get('Tên truyện')}: {e}")
        return None
        
    return story

def parse_relative_time(time_text):
    """Phân tích thời gian tương đối thành đối tượng datetime"""
    if not time_text or not isinstance(time_text, str) or not time_text.strip():
        return datetime.now()
        
    time_text = time_text.strip().lower()
    
    try:
        now = datetime.now()
        
        if "vừa xong" in time_text or "giây trước" in time_text:
            return now
            
        digits = ''.join(filter(str.isdigit, time_text))
        if not digits:
            return now  
        
        try:
            number = int(digits)
        except ValueError:
            return now
        
        if "phút trước" in time_text:
            return now - timedelta(minutes=number)
            
        if "giờ trước" in time_text:
            return now - timedelta(hours=number)
            
        if "ngày trước" in time_text:
            return now - timedelta(days=number)
            
        if "tuần trước" in time_text:
            return now - timedelta(weeks=number)
            
        if "tháng trước" in time_text:
            return now - timedelta(days=int(number * 30.44))
            
        if "năm trước" in time_text:
            return now - timedelta(days=number * 365)
            
        # Thử các định dạng chuỗi thời gian khác nhau
        date_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y"
        ]
        
        for format_str in date_formats:
            try:
                return datetime.strptime(time_text, format_str)
            except ValueError:
                continue
                
        return now
        
    except Exception as e:
        logger.error(f"Lỗi khi phân tích thời gian '{time_text}': {e}")
        return datetime.now()

class NetTruyenCrawler(BaseCrawler):
    """Crawler cho website NetTruyen sử dụng SeleniumBase và multiprocessing"""
    
    def __init__(self, db_manager, config_manager, base_url="https://nettruyenvia.com", max_pages=None, worker_count=5, start_page=1, end_page=None):
        super().__init__(db_manager, config_manager)
        
        # Đặt base_url từ tham số hoặc giá trị mặc định
        self.base_url = base_url if base_url else "https://nettruyenvia.com"
        self.max_pages = max_pages
        self.start_page = start_page
        self.end_page = end_page
        
        # Nếu không có end_page, tính từ start_page và max_pages
        if self.end_page is None and self.max_pages:
            self.end_page = self.start_page + self.max_pages - 1
        
        # Giới hạn số lượng worker dựa trên CPU và RAM
        cpu_count = multiprocessing.cpu_count()
        available_workers = max(1, worker_count)
        self.worker_count = min(available_workers, MAX_DRIVER_INSTANCES)
        logger.info(f"Khởi tạo với {self.worker_count} workers (Từ {worker_count} yêu cầu, {cpu_count} CPU)")
        
        # Khởi tạo SQLiteHelper
        self.sqlite_helper = SQLiteHelper(self.db_manager.db_folder)
        
        # User agent để tránh bị chặn
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        
        # Biến theo dõi tiến độ
        self.total_comics = 0
        self.processed_comics = Value('i', 0)
    
    @retry(max_retries=2)
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản từ NetTruyen với multiprocessing"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Đảm bảo multiprocessing hoạt động đúng trên các nền tảng khác nhau
            if not hasattr(multiprocessing, 'get_start_method') or multiprocessing.get_start_method() != 'spawn':
                try:
                    multiprocessing.set_start_method('spawn', force=True)
                except RuntimeError:
                    # Đã đặt ở một nơi khác, bỏ qua
                    pass
            
            # Đặt nguồn dữ liệu
            self.db_manager.set_source("NetTruyen")
            
            # Lấy danh sách truyện (sử dụng driver riêng)
            driver = None
            raw_comics = []
            
            try:
                driver = setup_driver()
                raw_comics = self.get_all_stories(driver, self.max_pages, progress_callback)
                logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
                self.total_comics = len(raw_comics)
            except Exception as e:
                logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
                if not raw_comics:
                    raise  # Không có dữ liệu để xử lý, kết thúc
            finally:
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
            
            # Nếu không lấy được truyện nào, kết thúc
            if not raw_comics:
                logger.warning("Không lấy được truyện nào, kết thúc quá trình crawl")
                return {"count": 0, "time_taken": time.time() - start_time, "website": "NetTruyen"}
            
            batch_size = min(50, len(raw_comics))
            
            for i in range(0, len(raw_comics), batch_size):
                # Kiểm tra tài nguyên hệ thống trước khi bắt đầu batch mới
                if not check_system_resources():
                    logger.warning("Tài nguyên hệ thống thấp, tạm dừng để phục hồi")
                    time.sleep(10)  
                
                batch = raw_comics[i:i+batch_size]
                logger.info(f"Xử lý batch {i//batch_size + 1}/{(len(raw_comics)-1)//batch_size + 1} ({len(batch)} truyện)")
                
                # Chuẩn bị tham số cho worker
                worker_params = [(comic, self.db_manager.db_folder, self.base_url, idx) for idx, comic in enumerate(batch)]
                
                # Tạo và quản lý pool processes
                try:
                    # Số lượng process động dựa trên tình trạng hệ thống
                    dynamic_worker_count = max(self.worker_count, 1)
                    
                    with Pool(processes=dynamic_worker_count, initializer=init_process, maxtasksperchild=3) as pool:
                        try:
                            # Sử dụng map thay vì map_async để đơn giản hóa
                            results = pool.map(process_comic_worker, worker_params, chunksize=1)
                            
                            # Lọc ra các kết quả không None
                            valid_results = [r for r in results if r is not None]
                            
                            # Cập nhật số lượng truyện đã xử lý
                            batch_comics_count = len(valid_results)
                            comics_count += batch_comics_count
                            
                            # Cập nhật biến đếm shared
                            with self.processed_comics.get_lock():
                                self.processed_comics.value += batch_comics_count
                            
                            logger.info(f"Kết thúc batch {i//batch_size + 1}: Đã xử lý {batch_comics_count}/{len(batch)} truyện trong batch")
                            
                            # Cập nhật tiến độ
                            if progress_callback and len(raw_comics) > 0:
                                progress = (self.processed_comics.value / len(raw_comics)) * 100
                                progress_callback.emit(int(min(progress, 100)))
                            
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý map trong pool: {e}")
                            
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý batch: {e}")
                
                # Gọi garbage collector
                gc.collect()
                
                # Pause nhỏ giữa các batch để giải phóng tài nguyên
                logger.info("Nghỉ giữa các batch để giải phóng tài nguyên...")
                time.sleep(3)
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình crawl: {e}")
        finally:
            # Đóng tất cả kết nối SQLite trong thread hiện tại
            try:
                self.sqlite_helper.close_all_connections()
            except:
                pass
            
            # Thu gom rác một lần nữa
            gc.collect()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Đã crawl {comics_count} truyện trong {elapsed_time:.2f} giây")
        
        return {
            "count": comics_count,
            "time_taken": time.time() - start_time,
            "website": "NetTruyen"
        }
    
    @retry(max_retries=2)
    def get_all_stories(self, driver, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        stories = []
        
        try:
            # Sử dụng start_page và end_page nếu có, ngược lại dùng max_pages
            if self.start_page and self.end_page:
                start_page = self.start_page
                end_page = self.end_page
                logger.info(f"Sử dụng phạm vi trang từ {start_page} đến {end_page}")
            else:
                # Logic cũ để tương thích
                max_pages = max_pages if max_pages else self.max_pages if self.max_pages else 10
                start_page = 1
                end_page = max_pages
                logger.info(f"Sử dụng logic cũ: crawl {max_pages} trang từ trang 1")
            
            # Bypass Cloudflare trước tiên
            bypass_cloudflare(driver, self.base_url)
            
            # Duyệt qua từng trang trong phạm vi đã định
            for page in range(start_page, end_page + 1):
                if not check_system_resources():
                    logger.warning("Tài nguyên hệ thống thấp, tạm dừng trước khi tải trang tiếp theo")
                    time.sleep(5)  # Đợi hệ thống phục hồi
                
                url = f"{self.base_url}/?page={page}"
                logger.info(f"Đang tải trang {page}: {url}")
                
                try:
                    driver.get(url)
                except WebDriverException as e:
                    logger.error(f"Lỗi khi truy cập URL {url}: {e}")
                    # Thử lại với backoff
                    time.sleep(random.uniform(3, 6))
                    try:
                        driver.get(url)
                    except:
                        continue  # Tiếp tục với trang tiếp theo
                        
                time.sleep(random.uniform(2, 4))

                try:
                    # Đợi để trang tải xong
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".items .row .item"))
                    )
                except Exception:
                    logger.info(f"Không tìm thấy phần tử truyện trên trang {page}, kết thúc")
                    break

                # Lấy tất cả các item truyện
                item_elements = driver.find_elements("css selector", ".items .row .item")
                
                if not item_elements:
                    logger.info("Không tìm thấy truyện nào trên trang, kết thúc")
                    break
                    
                for item in item_elements:
                    try:
                        # Lấy tiêu đề và link truyện
                        title_element = item.find_element("css selector", "figcaption h3 a")
                        title = title_element.text.strip() if title_element.text else "Không có tên"
                        link = title_element.get_attribute("href")
                        
                        # Lấy thông tin chương
                        chapter_info = "Chapter 0"  # Giá trị mặc định
                        try:
                            chapter_info_elements = item.find_elements("css selector", "figcaption ul li a")
                            if chapter_info_elements:
                                chapter_info = chapter_info_elements[0].get_attribute("title") or "Chapter 0"
                        except Exception:
                            pass
                        
                        # Trích xuất số chương
                        chapter_count = extract_chapter_number(chapter_info)
                        
                        if link:
                            stories.append({
                                "Tên truyện": title, 
                                "Link truyện": link,
                                "Số chương": chapter_count
                            })
                    except StaleElementReferenceException:
                        logger.warning("Phần tử không còn tồn tại trong DOM, bỏ qua")
                        continue
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý truyện: {e}")
                        continue

                # Cập nhật tiến độ
                if progress_callback:
                    total_pages = end_page - start_page + 1
                    current_progress = (page - start_page + 1) / total_pages
                    progress = min(25, current_progress * 25)  # Chỉ chiếm 25% đầu tiên
                    progress_callback.emit(int(progress))

                time.sleep(random.uniform(2, 3))
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
            
        # Kiểm tra lại danh sách và loại bỏ các mục không hợp lệ
        valid_stories = [story for story in stories 
                        if story.get("Tên truyện") and story.get("Link truyện") 
                        and story.get("Link truyện").startswith("http")]
        
        logger.info(f"Đã tìm thấy {len(valid_stories)} truyện hợp lệ để crawl (từ {len(stories)} kết quả ban đầu)")
        return valid_stories
    
    @retry(max_retries=2)
    def crawl_comments(self, comic, time_limit=None, days_limit=None):
        """Crawl comment cho một truyện cụ thể với giới hạn thời gian"""
        driver = setup_driver()
        comments = []
        unique_contents = set()
        old_comments_count = 0
        
        try:

            
            # Lấy link từ comic
            link = comic.get("link_truyen")
            bypass_cloudflare(driver, link)
            if not link:
                logger.error(f"Không tìm thấy link truyện cho: {comic.get('ten_truyen')}")
                driver.quit()
                return []
            
            # Log thông tin về giới hạn thời gian
            if time_limit:
                logger.info(f"Crawl comment cho truyện: {comic.get('ten_truyen')} từ {days_limit} ngày gần đây ({time_limit.strftime('%Y-%m-%d')})")
            else:
                logger.info(f"Crawl tất cả comment cho truyện: {comic.get('ten_truyen')}")
            
            # Truy cập trang và chuyển đến phần comment
            driver.get(link)
            time.sleep(random.uniform(2, 3))
            try:
                driver.execute_script("joinComment()")
                time.sleep(random.uniform(2, 3))
            except:
                logger.warning("Không thể gọi hàm joinComment, trang có thể không có phần comment")
                    
            page_comment = 1
            max_comment_pages = 100  
            stop_crawling = False
            
            while page_comment <= max_comment_pages and not stop_crawling:
                comments_in_current_page = 0
                old_comments_in_page = 0
                
                # Lấy elements comments với retry
                comment_elements = []
                for retry in range(3):  # Thử tối đa 3 lần
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".comment-list"))
                        )
                        comment_elements = driver.find_elements("css selector", ".comment-list .item.clearfix .info")
                        if comment_elements:
                            break
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Thử lấy comments lần {retry+1}: {e}")
                        time.sleep(1)
                
                if not comment_elements:
                    logger.info(f"Không tìm thấy comment nào trên trang {page_comment}, dừng crawl")
                    break
                    
                total_comments_in_page = len(comment_elements)
                logger.info(f"Tìm thấy {total_comments_in_page} comment trên trang {page_comment}")
                
                # Xử lý từng comment
                for comment in comment_elements:
                    try:
                        # Lấy tên người bình luận
                        try:
                            name_elem = comment.find_element("css selector", ".comment-header span.authorname")
                            name = name_elem.text.strip() if name_elem.text else "Người dùng ẩn danh"
                        except:
                            name = "Người dùng ẩn danh"
                        
                        # Lấy nội dung bình luận
                        try:
                            content_elem = comment.find_element("css selector", ".comment-content")
                            content = content_elem.text.strip() if content_elem.text else "N/A"
                        except:
                            content = "N/A"
                        
                        time_text = ""
                        try:
                            # Thử nhiều selector cho thời gian comment
                            selectors = [
                                "ul.comment-footer .li .abbr",
                                "ul.comment-footer li abbr",
                                ".comment-header abbr"
                            ]
                            
                            for selector in selectors:
                                try:
                                    abbr_elem = comment.find_element("css selector", selector)
                                    time_text = abbr_elem.get_attribute("title") or abbr_elem.text.strip()
                                    if time_text:
                                        break
                                except:
                                    continue
                                    
                            if time_text:
                                logger.info(f"Thời gian comment raw: '{time_text}'")
                        except:
                            logger.debug("Không tìm thấy thẻ chứa thời gian")
                        
                        # Xử lý thời gian
                        comment_time = datetime.now() 
                        if time_text:
                            comment_time = self.parse_relative_time(time_text)
                        
                        # Kiểm tra giới hạn thời gian
                        if time_limit and comment_time < time_limit:
                            logger.info(f"Comment quá cũ: '{time_text}' ({comment_time.strftime('%Y-%m-%d')} < {time_limit.strftime('%Y-%m-%d')})")
                            old_comments_count += 1
                            stop_crawling = True
                            break
                        
                        # Kiểm tra trùng lặp (chỉ với nội dung có ý nghĩa)
                        if content != "N/A" and content in unique_contents:
                            logger.info("Phát hiện comment trùng lặp, dừng crawl")
                            stop_crawling = True
                            break
                        
                        if content != "N/A" and len(content) > 5:
                            unique_contents.add(content)
                        
                        # Thêm comment vào danh sách kết quả
                        comments.append({
                            "ten_nguoi_binh_luan": name,
                            "noi_dung": content,
                            "comic_id": comic.get("id"),
                            "thoi_gian_binh_luan": comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                            "thoi_gian_cap_nhat": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        
                        comments_in_current_page += 1
                        
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý comment: {e}")
                
                # Thống kê kết quả trang hiện tại
                logger.info(f"Trang {page_comment}: {comments_in_current_page} comment mới, {old_comments_in_page} comment quá cũ")
                
                if stop_crawling:
                    logger.info("Dừng crawl do nhiều comment quá cũ")
                    break
                
                # Tìm nút Next bằng nhiều cách khác nhau
                next_button = None
                try:
                    # Sử dụng nhiều selector có thể để tìm nút Next
                    next_button_selectors = [
                        # Class-based selectors (ưu tiên)
                        "ul.pagination li.active + li a",
                        "ul.pagination li a[title='Trang sau']",
                        "a.next-page",
                        # Text-based selectors
                        "//a[contains(text(), 'Next')]",
                        "//a[contains(text(), 'Tiếp')]",
                        "//a[contains(text(), '>')]",
                    ]
                    
                    for selector in next_button_selectors:
                        try:
                            if selector.startswith("//"):
                                # XPath selector
                                elements = driver.find_elements("xpath", selector)
                            else:
                                # CSS selector
                                elements = driver.find_elements("css selector", selector)
                                
                            if elements:
                                next_button = elements[0]
                                logger.info(f"Đã tìm thấy nút chuyển trang với selector: {selector}")
                                break
                        except Exception:
                            continue
                            
                    if next_button:
                        # Scroll đến nút trước khi nhấp
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                        time.sleep(1)  # Đợi sau khi scroll
                        
                        # Thử click an toàn
                        try:
                            # Ưu tiên JavaScript click vì an toàn hơn
                            driver.execute_script("arguments[0].click();", next_button)
                            logger.info(f"Đã click nút Next bằng JavaScript")
                        except Exception as e1:
                            try:
                                next_button.click()
                                logger.info(f"Đã click nút Next thông thường")
                            except Exception as e2:
                                logger.warning(f"Không thể click nút Next: JS error: {e1}, regular error: {e2}")
                                # Thử một cách khác: mở URL trực tiếp
                                try:
                                    next_url = next_button.get_attribute("href")
                                    if next_url:
                                        driver.get(next_url)
                                        logger.info(f"Đã chuyển trang bằng URL: {next_url}")
                                    else:
                                        logger.error("Không thể lấy URL từ nút Next")
                                        break
                                except:
                                    # Không thể tiếp tục
                                    logger.error("Không thể chuyển trang, dừng crawl")
                                    break
                        
                        page_comment += 1
                        logger.info(f"Chuyển sang trang comment {page_comment}")
                        time.sleep(random.uniform(2, 3))
                    else:
                        logger.info("Không tìm thấy nút chuyển trang, kết thúc crawl")
                        break
                except Exception as e:
                    logger.error(f"Lỗi khi tìm/click nút chuyển trang: {e}")
                    break

        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {e}")
        finally:
            if driver:
                driver.quit()
            
        if time_limit:
            logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')} (bỏ qua {old_comments_count} comment quá cũ)")
        else:
            logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')}")
        
        return comments
    
    def crawl_comments_batch(self, comics_list, progress_callback=None):
        """
        Crawl comments cho danh sách truyện sử dụng multiprocessing + multithreading
        
        Args:
            comics_list: Danh sách truyện cần crawl comments
            progress_callback: Callback để báo cáo tiến trình
            
        Returns:
            dict: Kết quả crawl
        """
        logger.info(f"Bắt đầu crawl comments batch cho {len(comics_list)} truyện NetTruyen")
        
        # Sử dụng CommentCrawler từ base class
        return self.crawl_comments_parallel(comics_list, progress_callback)

# Import cần thiết, thêm vào phần đầu nếu cần
import sys
import traceback

# Đặt exception hook để ghi log lỗi không được xử lý
def global_exception_handler(exctype, value, tb):
    logger.critical("Lỗi không bắt được: %s", ''.join(traceback.format_exception(exctype, value, tb)))
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = global_exception_handler