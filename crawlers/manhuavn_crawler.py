from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
from datetime import datetime, timedelta
import re
import os
import gc
import signal
import psutil
import multiprocessing
from multiprocessing import Pool, Value, current_process
from functools import wraps
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, StaleElementReferenceException
from utils.sqlite_helper import SQLiteHelper

# Thiết lập logging
logger = logging.getLogger(__name__)

# Thiết lập giới hạn tài nguyên
MAX_MEMORY_PERCENT = 80  # Giới hạn % RAM sử dụng
MAX_DRIVER_INSTANCES = 25  # Giới hạn số lượng driver đồng thời
DEFAULT_TIMEOUT = 30  # Timeout mặc định (giây)
MAX_RETRIES = 3  # Số lần thử lại tối đa

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
            
            # Xử lý truyện
            try:
                result = crawl_comic_details(comic, driver, worker_id)
                if result:
                    # Lưu vào database
                    try:
                        sqlite_helper.save_comic_to_db(result, "Manhuavn")
                        logger.info(f"Worker {worker_id}: Hoàn thành lưu truyện {comic.get('Tên truyện', '')}")
                    except Exception as e:
                        logger.error(f"Worker {worker_id}: Lỗi khi lưu vào database: {e}")
                return result
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

@retry(max_retries=MAX_RETRIES)
def crawl_comic_details(comic, driver, worker_id=0):
    """Crawl chi tiết của một truyện"""
    try:
        # Tạo URL an toàn
        url = comic.get("Link truyện", "")
        if not url.startswith("http"):
            logger.warning(f"Worker {worker_id}: URL không hợp lệ: {url}")
            return None
            
        # Giới hạn số lần thử truy cập URL
        for attempt in range(5):
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".info-row .contiep"))
                )
                break
            except TimeoutException:
                logger.warning(f"Worker {worker_id}: Timeout khi truy cập {url}, thử lần {attempt + 1}/5")
                time.sleep(random.uniform(2, 4))
            except WebDriverException as e:
                logger.warning(f"Worker {worker_id}: Lỗi WebDriver khi truy cập {url}, thử lần {attempt + 1}/5: {e}")
                time.sleep(random.uniform(2, 4))
            except Exception as e:
                logger.warning(f"Worker {worker_id}: Lỗi không xác định khi truy cập {url}, thử lần {attempt + 1}/5: {e}")
                time.sleep(random.uniform(2, 4))
        else:
            logger.error(f"Worker {worker_id}: Không thể truy cập trang sau 5 lần thử: {url}")
            return None

        # Lấy thông tin chi tiết
        story = {}
        story["Tên truyện"] = comic.get("Tên truyện", "Không có tên")
        story["Link truyện"] = url
        
        # Lấy thông tin chi tiết với xử lý ngoại lệ chi tiết
        try:
            story["Tình trạng"] = get_text_safe(driver, ".info-row .contiep")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy tình trạng: {e}")
            story["Tình trạng"] = "N/A"
            
        try:
            story["Lượt theo dõi"] = get_text_safe(driver, "li.info-row strong")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt theo dõi: {e}")
            story["Lượt theo dõi"] = "0"
            
        try:
            story["Lượt xem"] = parse_number(get_text_safe(driver, "li.info-row view.colorblue"))
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt xem: {e}")
            story["Lượt xem"] = "0"
            
        try:
            story["Đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingValue"]')
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy đánh giá: {e}")
            story["Đánh giá"] = "0"
            
        try:
            story["Lượt đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingCount"]')
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy lượt đánh giá: {e}")
            story["Lượt đánh giá"] = "0"
            
        try:
            story["Mô tả"] = get_text_safe(driver, "li.clearfix p")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy mô tả: {e}")
            story["Mô tả"] = ""

        # Lấy số chương
        try:
            chapter_text = get_text_safe(driver, "li.info-row a.colorblue")
            chapter_match = re.search(r'\d+', chapter_text)
            story["Số chương"] = chapter_match.group() if chapter_text != "N/A" and chapter_match else "0"
        except Exception as e:
            logger.warning(f"Worker {worker_id}: Không thể lấy số chương: {e}")
            story["Số chương"] = "0"
        
        # Lấy tác giả
        try:
            author_element = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/ul/li[6]/a")
            story["Tác giả"] = author_element.text.strip()
        except (NoSuchElementException, StaleElementReferenceException):
            try:
                # Thử cách khác để tìm tác giả
                author_elements = driver.find_elements(By.CSS_SELECTOR, ".info-row a")
                for elem in author_elements:
                    if "tác giả" in elem.get_attribute("title").lower():
                        story["Tác giả"] = elem.text.strip()
                        break
                else:
                    story["Tác giả"] = "N/A"
            except:
                story["Tác giả"] = "N/A"
                
        # Chuyển đổi sang định dạng database
        db_comic = {
            "ten_truyen": story.get("Tên truyện", ""),
            "tac_gia": story.get("Tác giả", "N/A"),
            "mo_ta": story.get("Mô tả", ""),
            "link_truyen": story.get("Link truyện", ""),
            "so_chuong": int(story.get("Số chương", "0")) if story.get("Số chương", "0").isdigit() else 0,
            "luot_xem": parse_number(story.get("Lượt xem", "0")),
            "luot_theo_doi": extract_number(story.get("Lượt theo dõi", "0")),
            "danh_gia": story.get("Đánh giá", "0"),
            "luot_danh_gia": extract_number(story.get("Lượt đánh giá", "0")),
            "trang_thai": story.get("Tình trạng", ""),
            "nguon": "Manhuavn"
        }
        
        logger.info(f"Worker {worker_id}: Hoàn thành thu thập dữ liệu cho truyện: {story.get('Tên truyện', '')}")
        return db_comic
            
    except Exception as e:
        logger.error(f"Worker {worker_id}: Lỗi không xử lý được khi lấy chi tiết truyện {comic.get('Tên truyện', '')}: {e}")
        raise  # Để retry decorator có thể xử lý

def get_text_safe(element, selector, default="N/A"):
    """Trích xuất nội dung văn bản an toàn từ phần tử"""
    try:
        elements = element.find_elements(By.CSS_SELECTOR, selector)
        if not elements:
            return default
        text = elements[0].text.strip()
        return text if text else default
    except (NoSuchElementException, StaleElementReferenceException):
        return default
    except Exception:
        return default

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
    """Trích xuất số từ các chuỗi với nhiều định dạng"""
    if not text_value or text_value == "N/A":
        return 0
        
    # Chỉ lấy phần số từ chuỗi
    try:
        # Lấy tất cả các số từ chuỗi
        numbers = re.findall(r'\d+', text_value)
        if numbers:
            # Lấy số lớn nhất nếu có nhiều số
            return max(map(int, numbers))
        return 0
    except Exception:
        return 0

def setup_driver():
    """Cấu hình Chrome WebDriver tối ưu"""
    chrome_options = Options()
    
    # Chạy chế độ headless nếu không cần giao diện
    chrome_options.add_argument("--headless")  
    
    # Tắt các tính năng không cần thiết
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    chrome_options.add_argument("--disable-usb") 
    chrome_options.add_argument("--disable-features=WebUSB,UsbChooserUI")
    
    # Giảm tài nguyên tiêu thụ
    chrome_options.add_argument("--memory-model=low")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-popup-blocking")
    
    # Bỏ qua cảnh báo automation
    chrome_options.add_experimental_option('excludeSwitches', ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Vô hiệu hóa logging để tránh crash
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
    os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
    os.environ['TF_USE_LEGACY_CPU'] = '0'
    os.environ['TF_DISABLE_MKL'] = '1'
    os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning,ignore::UserWarning'

    try:
        service = Service(log_path=os.devnull)  
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Thêm timeout
        driver.set_page_load_timeout(DEFAULT_TIMEOUT)
        driver.set_script_timeout(DEFAULT_TIMEOUT)
        return driver
    except Exception as e:
        logger.error(f"Lỗi khi khởi tạo Chrome driver: {e}")
        raise RuntimeError(f"Không thể khởi tạo Chrome driver: {e}")

def parse_relative_time(time_text):
    """Phân tích thời gian tương đối thành đối tượng datetime"""
    if not time_text or not isinstance(time_text, str) or not time_text.strip():
        return datetime.now()
        
    time_text = time_text.strip().lower()
    
    try:
        # Xử lý các định dạng thời gian đặc biệt
        if " - " in time_text and "/" in time_text:
            time_parts = time_text.split(" - ")
            if len(time_parts) == 2:
                time_part = time_parts[0].strip()  
                date_part = time_parts[1].strip()  
                
                # Tách ngày thành các thành phần
                date_components = date_part.split("/")
                if len(date_components) == 3:
                    day = date_components[0].zfill(2)
                    month = date_components[1].zfill(2)
                    year = date_components[2]
                    
                    # Chuyển đổi sang định dạng ISO
                    formatted_time = f"{year}-{month}-{day} {time_part}:00"
                    try:
                        return datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        pass
        
        # Xử lý thời gian tương đối
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

class ManhuavnCrawler(BaseCrawler):
    """Crawler cho trang Manhuavn sử dụng multiprocessing"""
    
    def __init__(self, db_manager, config_manager, base_url="https://manhuavn.top", max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        
        # Đặt base_url từ tham số hoặc giá trị mặc định
        self.base_url = base_url if base_url else "https://manhuavn.top"
        self.max_pages = max_pages
        
        # Giới hạn số lượng worker dựa trên CPU và RAM
        cpu_count = multiprocessing.cpu_count()
        available_workers = max(1, worker_count)
        self.worker_count = min(available_workers, MAX_DRIVER_INSTANCES)
        logger.info(f"Khởi tạo với {self.worker_count} workers (Từ {worker_count} yêu cầu, {cpu_count} CPU)")
        
        # Khởi tạo SQLiteHelper
        self.sqlite_helper = SQLiteHelper(self.db_manager.db_folder)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        
        # Biến theo dõi tiến độ
        self.total_comics = 0
        self.processed_comics = Value('i', 0)
    
    @retry(max_retries=2)
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản từ Manhuavn với multiprocessing"""
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
                return {"count": 0, "time_taken": time.time() - start_time, "website": "Manhuavn"}
                
            batch_size = min(50, len(raw_comics))
            
            for i in range(0, len(raw_comics), batch_size):
                # Kiểm tra tài nguyên hệ thống trước khi bắt đầu batch mới
                if not check_system_resources():
                    logger.warning("Tài nguyên hệ thống thấp, tạm dừng để phục hồi")
                    time.sleep(10)  # Đợi hệ thống phục hồi
                
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
            "website": "Manhuavn"
        }
    
    @retry(max_retries=2)
    def get_all_stories(self, driver, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        stories = []
        page = 1
        
        try:
            # Giới hạn số trang nếu không được chỉ định
            if max_pages is None:
                max_pages = 10  # Giá trị mặc định an toàn
            
            while page <= max_pages:
                url = f"{self.base_url}/danhsach/P{page}/index.html?status=0&sort=2"
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
                        break
                
                time.sleep(random.uniform(2, 4))

                # Sử dụng try-except riêng cho việc đợi phần tử
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".lst_story .story_item"))
                    )
                except TimeoutException:
                    logger.info(f"Timeout khi chờ phần tử truyện trên trang {page}, thử phương pháp khác")
                    # Thử các selector khác nếu cần
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, ".lst_story"))
                        )
                        # Kiểm tra trực tiếp nếu có phần tử
                        if not driver.find_elements(By.CSS_SELECTOR, ".story_item"):
                            logger.info(f"Không tìm thấy phần tử truyện trên trang {page}, kết thúc")
                            break
                    except:
                        logger.info(f"Không tìm thấy phần tử truyện trên trang {page}, kết thúc")
                        break

                # Lấy tất cả các item truyện
                try:
                    item_elements = driver.find_elements(By.CSS_SELECTOR, ".lst_story .story_item")
                    
                    if not item_elements:
                        logger.info("Không tìm thấy truyện nào trên trang, kết thúc")
                        break
                        
                    for item in item_elements:
                        try:
                            # Lấy tiêu đề và link truyện
                            title_element = item.find_elements(By.CSS_SELECTOR, ".story_title")
                            if title_element:
                                title = title_element[0].text.strip()
                            else:
                                title = "Không có tên"
                                
                            link_element = item.find_elements(By.CSS_SELECTOR, "a")
                            if link_element:
                                link = link_element[0].get_attribute("href")
                                if link and title:
                                    stories.append({
                                        "Tên truyện": title, 
                                        "Link truyện": link
                                    })
                        except StaleElementReferenceException:
                            logger.warning("Phần tử không còn tồn tại trong DOM, bỏ qua")
                            continue
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý truyện: {e}")
                            continue

                    # Cập nhật tiến độ
                    if progress_callback and max_pages:
                        progress = min(25, (page / max_pages) * 25) 
                        progress_callback.emit(int(progress))

                    # Tăng số trang
                    page += 1
                    
                    # Ngủ để tránh tải quá nhanh
                    time.sleep(random.uniform(2, 3))
                    
                except Exception as e:
                    logger.error(f"Lỗi khi lấy danh sách truyện từ trang {page}: {e}")
                    # Thử trang tiếp theo
                    page += 1
                
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
        """Crawl comments cho một truyện cụ thể"""
        driver = None
        comments = []
        old_comments_count = 0
        
        try:
            # Lấy link từ comic
            link = comic.get("link_truyen")
            comic_id = comic.get("id")
            
            if not link or not comic_id:
                logger.error(f"Không tìm thấy link hoặc ID truyện: {comic.get('ten_truyen', 'Unknown')}")
                return []
            
            if time_limit:
                logger.info(f"Crawl comment cho truyện: {comic.get('ten_truyen')} từ {days_limit} ngày gần đây ({time_limit.strftime('%Y-%m-%d')})")
            else:
                logger.info(f"Crawl tất cả comment cho truyện: {comic.get('ten_truyen')}")
            
            # Khởi tạo driver với kiểm tra tài nguyên
            if not check_system_resources():
                logger.warning("Tài nguyên hệ thống thấp, tạm dừng trước khi tạo driver")
                time.sleep(5)
                gc.collect()
                
            # Khởi tạo driver
            try:
                driver = setup_driver()
            except Exception as e:
                logger.error(f"Không thể tạo driver cho crawl comment: {e}")
                return []
            
            try:
                driver.get(link)
            except WebDriverException as e:
                logger.error(f"Lỗi khi truy cập URL {link}: {e}")
                if driver:
                    driver.quit()
                return []
            
            time.sleep(random.uniform(2, 3))
            load_more_attempts = 0
            stop_loading = False
            max_load_attempts = 20  
            
            while load_more_attempts < max_load_attempts and not stop_loading:
                try:
                    # Kiểm tra RAM trước khi tiếp tục
                    if load_more_attempts > 0 and load_more_attempts % 5 == 0:
                        if not check_system_resources():
                            logger.warning("Tài nguyên hệ thống thấp, dừng tải thêm comment")
                            break
                    
                    # Kiểm tra thời gian của comment cuối nếu có giới hạn thời gian
                    if time_limit:
                        try:
                            current_comments = driver.find_elements(By.CSS_SELECTOR, ".comment_item")
                            
                            # Kiểm tra comment cuối cùng 
                            if current_comments:
                                last_comment = current_comments[-1]
                                try:
                                    time_span = last_comment.find_element(By.CSS_SELECTOR, "div.comment-head > span.time")
                                    time_text = time_span.get_attribute("datetime") or time_span.text.strip()
                                    
                                    if time_text:
                                        comment_time = parse_relative_time(time_text)
                                        
                                        # Kiểm tra nếu comment cuối cùng đã quá cũ
                                        if comment_time < time_limit:
                                            logger.info(f"Dừng tải thêm comment: Đã phát hiện comment quá cũ ({time_text})")
                                            stop_loading = True
                                            break
                                except (NoSuchElementException, StaleElementReferenceException) as e:
                                    logger.debug(f"Không thể lấy thời gian của comment cuối: {e}")
                        except Exception as e:
                            logger.warning(f"Lỗi khi kiểm tra thời gian comment: {e}")
                    
                    # Thử nhiều cách để tìm và nhấp vào nút "Xem thêm"
                    button_found = False
                    button_options = [
                        (By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button"),
                        (By.CSS_SELECTOR, ".load-more-comm"),
                        (By.XPATH, "//button[contains(text(), 'Xem thêm')]"),
                        (By.XPATH, "//button[contains(@class, 'load')]")
                    ]
                    
                    for selector_type, selector in button_options:
                        try:
                            elements = driver.find_elements(selector_type, selector)
                            if elements and elements[0].is_displayed():
                                # Scroll đến nút
                                driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", elements[0])
                                time.sleep(0.5)
                                
                                # Nhấp vào nút bằng JavaScript để tránh lỗi "element not clickable"
                                driver.execute_script("arguments[0].click();", elements[0])
                                button_found = True
                                load_more_attempts += 1
                                logger.debug(f"Đã nhấp nút 'Xem thêm' lần {load_more_attempts} với selector {selector}")
                                time.sleep(2)
                                break
                        except Exception:
                            continue
                    
                    if not button_found:
                        logger.info("Không tìm thấy nút 'Xem thêm', có thể đã tải hết comment")
                        break

                except Exception as e:
                    logger.debug(f"Không thể tải thêm comment: {e}")
                    break

            # Lấy danh sách tất cả các comment đã tải
            try:
                comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment_item")
                logger.info(f"Đã tìm thấy {len(comment_elements)} comment")
                
                # Xử lý từng comment
                for comment_elem in comment_elements:
                    try:
                        user = get_text_safe(comment_elem, ".comment-head")
                        content = get_text_safe(comment_elem, ".comment-content")
                        
                        time_text = ""
                        try:
                            time_span = comment_elem.find_element(By.CSS_SELECTOR, "div.comment-head > span.time")
                            time_text = time_span.get_attribute("datetime") or time_span.text.strip()
                        except (NoSuchElementException, StaleElementReferenceException):
                            pass
                        
                        comment_time = datetime.now()
                        if time_text:
                            comment_time = parse_relative_time(time_text)
                        
                        if time_limit and comment_time < time_limit:
                            logger.debug(f"Bỏ qua comment quá cũ: {time_text} ({comment_time.strftime('%Y-%m-%d')} < {time_limit.strftime('%Y-%m-%d')})")
                            old_comments_count += 1
                            continue
                        
                        # Làm sạch dữ liệu
                        if not content or content.strip() == "":
                            content = "N/A"
                            
                        if not user or user.strip() == "":
                            user = "Người dùng ẩn danh"
                            
                        # Loại bỏ các ký tự đặc biệt có thể gây lỗi SQL
                        content = content.replace("'", "''").replace('"', '""')
                        user = user.replace("'", "''").replace('"', '""')
                        
                        # Giới hạn độ dài để tránh lỗi database
                        if len(content) > 2000:
                            content = content[:1997] + "..."
                            
                        if len(user) > 100:
                            user = user[:97] + "..."
                        
                        comments.append({
                            "ten_nguoi_binh_luan": user,
                            "noi_dung": content,
                            "comic_id": comic_id,
                            "thoi_gian_binh_luan": comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                            "thoi_gian_cap_nhat": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        })
                        
                    except (StaleElementReferenceException, NoSuchElementException) as e:
                        logger.debug(f"Phần tử comment không còn tồn tại: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý comment: {e}")
                        continue
                
                # Lưu comments vào database theo batch để tránh lỗi
                if comments:
                    try:
                        logger.info(f"Lưu {len(comments)} comment cho truyện ID {comic_id}")
                        
                        # Lưu theo batch nhỏ để tránh lỗi
                        batch_size = 100
                        for i in range(0, len(comments), batch_size):
                            batch = comments[i:i+batch_size]
                            try:
                                self.sqlite_helper.save_comments_to_db(comic_id, batch, "Manhuavn")
                                logger.debug(f"Đã lưu batch {i//batch_size + 1}/{(len(comments)-1)//batch_size + 1} ({len(batch)} comment)")
                            except Exception as e:
                                logger.error(f"Lỗi khi lưu batch comment: {e}")
                    except Exception as e:
                        logger.error(f"Lỗi khi lưu comments vào database: {e}")
            except Exception as e:
                logger.error(f"Lỗi khi xử lý danh sách comment: {e}")
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {e}")
        finally:
            try:
                if driver:
                    driver.quit()
            except:
                pass
        
        if time_limit:
            logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')} (bỏ qua {old_comments_count} comment quá cũ)")
        else:
            logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')}")
        
        return comments

    def transform_comic_data(self, raw_comic):
        """Chuyển đổi dữ liệu raw sang định dạng database"""
        return {
            "ten_truyen": raw_comic.get("Tên truyện", ""),
            "tac_gia": raw_comic.get("Tác giả", "N/A"),
            "mo_ta": raw_comic.get("Mô tả", ""),
            "link_truyen": raw_comic.get("Link truyện", ""),
            "so_chuong": int(raw_comic.get("Số chương", "0")) if raw_comic.get("Số chương", "0").isdigit() else 0,
            "luot_xem": raw_comic.get("Lượt xem", "0"),
            "luot_theo_doi": extract_number(raw_comic.get("Lượt theo dõi", "0")),
            "danh_gia": raw_comic.get("Đánh giá", "0"),
            "luot_danh_gia": extract_number(raw_comic.get("Lượt đánh giá", "0")),
            "trang_thai": raw_comic.get("Tình trạng", ""),
            "nguon": "Manhuavn"
        }

# Import cần thiết, thêm vào phần đầu nếu cần
import sys
import traceback

# Đặt exception hook để ghi log lỗi không được xử lý
def global_exception_handler(exctype, value, tb):
    logger.critical("Lỗi không bắt được: %s", ''.join(traceback.format_exception(exctype, value, tb)))
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = global_exception_handler