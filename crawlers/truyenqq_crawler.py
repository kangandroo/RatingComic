from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
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
from datetime import datetime, timedelta

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
            logger.warning(f"Worker {worker_id}: Tài nguyên hệ thống không đủ, bỏ qua truyện {comic.get('ten_truyen', '')}")
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
                driver = create_chrome_driver()
                logger.debug(f"Worker {worker_id}: Đã tạo driver thành công")
            except Exception as e:
                logger.error(f"Worker {worker_id}: Không thể tạo driver: {e}")
                return None
            
            # Xử lý truyện
            try:
                comic_url = comic["link_truyen"]
                logger.debug(f"Worker {worker_id}: Đang crawl chi tiết truyện: {comic_url}")
                
                # Giới hạn số lần thử truy cập URL
                for attempt in range(5):
                    try:
                        driver.get(comic_url)
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "li.author.row p.col-xs-9 a"))
                        )
                        break
                    except TimeoutException:
                        logger.warning(f"Worker {worker_id}: Timeout khi truy cập {comic_url}, thử lần {attempt + 1}/5")
                        time.sleep(random.uniform(2, 4))
                    except WebDriverException as e:
                        logger.warning(f"Worker {worker_id}: Lỗi WebDriver khi truy cập {comic_url}, thử lần {attempt + 1}/5: {e}")
                        time.sleep(random.uniform(2, 4))
                    except Exception as e:
                        logger.warning(f"Worker {worker_id}: Lỗi không xác định khi truy cập {comic_url}, thử lần {attempt + 1}/5: {e}")
                        time.sleep(random.uniform(2, 4))
                else:
                    logger.error(f"Worker {worker_id}: Không thể truy cập trang sau 5 lần thử: {comic_url}")
                    return None
                
                # Kiểm tra xem có phần tử tên khác không
                try:
                    ten_khac_element = driver.find_elements(By.CSS_SELECTOR, "li.othername.row h2")
                    if ten_khac_element:
                        comic["ten_khac"] = ten_khac_element[0].text.strip()
                        comic["tac_gia"] = get_text_safe(driver, "li.author.row p.col-xs-9 a", "N/A")
                        comic["trang_thai"] = get_text_safe(driver, "li.status.row p.col-xs-9", "N/A")
                        comic["luot_thich"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9.number-like", "0")
                        comic["luot_theo_doi"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9", "0")
                        comic["luot_xem"] = get_text_safe(driver, "li:nth-child(6) p.col-xs-9", "0")
                    else:
                        comic["ten_khac"] = "Không có tên khác"
                        comic["tac_gia"] = get_text_safe(driver, "li.author.row p.col-xs-9 a", "N/A")
                        comic["trang_thai"] = get_text_safe(driver, "li.status.row p.col-xs-9", "N/A")
                        comic["luot_thich"] = get_text_safe(driver, "li:nth-child(3) p.col-xs-9.number-like", "0")
                        comic["luot_theo_doi"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9", "0")
                        comic["luot_xem"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9", "0")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Lỗi khi lấy thông tin cơ bản: {e}")
                
                # Cố gắng nhấp vào "Xem thêm" nếu có để lấy mô tả đầy đủ
                try:
                    readmore_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "p > a"))
                    )
                    readmore_button.click()
                    time.sleep(1)
                except Exception as e:
                    logger.debug(f"Worker {worker_id}: Không tìm thấy nút 'Xem thêm' hoặc không thể click: {e}")
                
                # Lấy mô tả
                try:
                    comic["mo_ta"] = get_text_safe(driver, "div.story-detail-info.detail-content", "")
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Không thể lấy mô tả: {e}")
                    comic["mo_ta"] = ""
                
                # Chuyển các giá trị sang số
                try:
                    comic["luot_xem"] = extract_number(comic["luot_xem"])
                    comic["luot_thich"] = extract_number(comic["luot_thich"])
                    comic["luot_theo_doi"] = extract_number(comic["luot_theo_doi"])
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Lỗi khi chuyển đổi giá trị số: {e}")
                
                # Lưu vào database
                try:
                    sqlite_helper.save_comic_to_db(comic, "TruyenQQ")
                    logger.info(f"Worker {worker_id}: Hoàn thành lưu truyện {comic.get('ten_truyen', '')}")
                except Exception as e:
                    logger.error(f"Worker {worker_id}: Lỗi khi lưu vào database: {e}")
                
                return comic
                
            except Exception as e:
                logger.error(f"Worker {worker_id}: Lỗi khi xử lý truyện {comic.get('ten_truyen', '')}: {e}")
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

# Các hàm trợ giúp định nghĩa ở cấp module
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
        
def extract_chapter_number(chapter_text):
    """Trích xuất số chương từ text (ví dụ: 'Chapter 124' -> 124)"""
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

def extract_number(text_value):
    """
    Trích xuất số từ chuỗi với nhiều định dạng
    Ví dụ: '1,234' -> 1234, '5K' -> 5000, '3.2M' -> 3200000
    """
    try:
        if not text_value or text_value == 'N/A':
            return 0
            
        text_value = str(text_value).strip()
        
        # Xử lý hậu tố K và M
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
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                return int(float(num_part) * 1000000)
            else:
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000000)
        else:
            # Xử lý số có nhiều dấu chấm là dấu phân cách hàng nghìn
            if text_value.count('.') > 1:
                text_value = text_value.replace('.', '')
            
            # Xử lý dấu phẩy là dấu phân cách hàng nghìn
            text_value = text_value.replace(',', '')
            
            return int(float(text_value))
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất số từ '{text_value}': {str(e)}")
        return 0

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

def create_chrome_driver():
    """Cấu hình Chrome WebDriver tối ưu"""
    chrome_options = Options()
    
    # Chạy chế độ headless nếu không cần giao diện
    chrome_options.add_argument("--headless")  
    
    # Tắt các tính năng không cần thiết
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-accelerated-2d-canvas")

    chrome_options.add_argument("--disable-usb") 
    chrome_options.add_argument("--disable-features=WebUSB,UsbChooserUI")
    
    # Giảm tài nguyên tiêu thụ
    chrome_options.add_argument("--memory-model=low")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-popup-blocking")
    
    # Bỏ qua cảnh báo automation
    chrome_options.add_experimental_option('excludeSwitches', ["enable-automation", "enable-logging"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Thiết lập không tải hình ảnh để giảm tài nguyên
    prefs = {
        'profile.default_content_settings.images': 2,
        'profile.managed_default_content_settings.images': 2,
        'plugins.plugins_disabled': ['Chrome PDF Viewer'],
        'hardware_acceleration_mode.enabled': False,
        'profile.hardware_acceleration_enabled': False,
    }
    chrome_options.add_experimental_option('prefs', prefs)

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
        try:
            # Fallback với ít tùy chọn hơn
            fallback_options = Options()
            fallback_options.add_argument("--headless")
            fallback_options.add_argument("--no-sandbox")
            fallback_options.add_argument("--disable-gpu")
            fallback_options.add_argument("--disable-dev-shm-usage")
            return webdriver.Chrome(options=fallback_options)
        except Exception as e2:
            logger.critical(f"Lỗi nghiêm trọng khi khởi tạo Chrome driver: {e2}")
            raise RuntimeError(f"Không thể khởi tạo Chrome driver: {e2}")

class TruyenQQCrawler(BaseCrawler):
    """Crawler cho trang TruyenQQ sử dụng multiprocessing"""
    
    def __init__(self, db_manager, config_manager, base_url=None, max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        
        # Đặt base_url từ tham số hoặc giá trị mặc định
        self.base_url = base_url if base_url else "https://truyenqqgo.com"
        self.max_pages = max_pages
        
        # Giới hạn số lượng worker dựa trên CPU và RAM
        cpu_count = multiprocessing.cpu_count()
        available_workers = max(1, min(cpu_count - 1, worker_count))
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
        
        logger.info(f"Khởi tạo TruyenQQCrawler với base_url={self.base_url}")
    
    @retry(max_retries=2)
    def get_comic_listings(self, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ các trang danh sách"""
        all_comics = []
        page_num = 1
        driver = None
        
        try:
            # Số trang tối đa
            max_pages = max_pages if max_pages else self.max_pages if self.max_pages else 10  # Giá trị mặc định an toàn
            
            # Khởi tạo driver riêng cho việc thu thập danh sách truyện
            driver = create_chrome_driver()
            
            # Duyệt qua từng trang
            while page_num <= max_pages:
                if not check_system_resources():
                    logger.warning("Tài nguyên hệ thống thấp, tạm dừng trước khi tải trang tiếp theo")
                    time.sleep(5)  # Đợi hệ thống phục hồi
                
                try:
                    url = f"{self.base_url}/truyen-moi-cap-nhat/trang-{page_num}.html"
                    logger.info(f"Đang crawl trang {page_num}: {url}")
                    
                    for attempt in range(3):
                        try:
                            driver.get(url)
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".book_name.qtip h3 a"))
                            )
                            break
                        except TimeoutException:
                            logger.warning(f"Timeout khi truy cập {url}, thử lần {attempt + 1}/3")
                            time.sleep(random.uniform(2, 4))
                        except WebDriverException as e:
                            logger.warning(f"Lỗi WebDriver khi truy cập {url}, thử lần {attempt + 1}/3: {e}")
                            time.sleep(random.uniform(2, 4))
                    else:
                        logger.error(f"Không thể truy cập trang sau 3 lần thử: {url}")
                        break
                    
                    # Lấy danh sách các khối truyện
                    story_blocks = driver.find_elements(By.CSS_SELECTOR, ".list_grid_out ul.list_grid li")
                    
                    # Nếu không tìm thấy truyện nào, thoát khỏi vòng lặp
                    if not story_blocks:
                        logger.info(f"Không tìm thấy truyện nào ở trang {page_num}. Có thể đã đến trang cuối cùng.")
                        break
                    
                    # Thu thập dữ liệu từ trang hiện tại
                    page_stories = []
                    
                    for story_block in story_blocks:
                        try:
                            # Lấy thông tin tên và link truyện
                            try:
                                name_elem = story_block.find_element(By.CSS_SELECTOR, ".book_name.qtip h3 a")
                                story_name = name_elem.get_attribute("title")
                                story_link = name_elem.get_attribute("href")
                            except (NoSuchElementException, StaleElementReferenceException) as e:
                                logger.debug(f"Lỗi khi lấy tên và link truyện: {e}")
                                continue
                            
                            # Lấy thông tin chương
                            chapter_info = "Chapter 0"  # Giá trị mặc định
                            try:
                                chapter_elements = story_block.find_elements(By.CSS_SELECTOR, ".last_chapter a")
                                if chapter_elements:
                                    chapter_info = chapter_elements[0].get_attribute("title") or "Chapter 0"
                            except (NoSuchElementException, StaleElementReferenceException) as e:
                                logger.debug(f"Lỗi khi lấy thông tin chương: {e}")
                            
                            # Trích xuất số chương
                            chapter_count = extract_chapter_number(chapter_info)
                            
                            # Tạo đối tượng truyện
                            comic_data = {
                                "ten_truyen": story_name,
                                "link_truyen": story_link,
                                "so_chuong": chapter_count,
                                "nguon": "TruyenQQ"
                            }
                            
                            page_stories.append(comic_data)
                        except StaleElementReferenceException:
                            logger.warning("Phần tử không còn tồn tại trong DOM, bỏ qua")
                            continue
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý truyện: {e}")
                            continue
                    
                    logger.info(f"Trang {page_num}: Đã tìm thấy {len(page_stories)} truyện")
                    all_comics.extend(page_stories)
                    
                    # Chuyển sang trang tiếp theo
                    page_num += 1
                    
                    # Cập nhật tiến trình
                    if progress_callback and max_pages:
                        progress = min(25, (page_num / max_pages) * 25)  # Giới hạn 25% cho giai đoạn crawl danh sách
                        progress_callback.emit(int(progress))
                    
                    # Nghỉ ngẫu nhiên giữa các yêu cầu để tránh bị chặn
                    time.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập trang {page_num}: {str(e)}")
                    time.sleep(random.uniform(3, 5))  # Thêm thời gian chờ trước khi thử trang tiếp theo
                    page_num += 1  # Thử trang tiếp theo
                
                # Gọi garbage collector sau mỗi trang
                gc.collect()
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {str(e)}")
            
        finally:
            # Đóng driver sau khi sử dụng xong
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            
            # Thu gom rác
            gc.collect()
                    
        # Kiểm tra lại danh sách và loại bỏ các mục không hợp lệ
        valid_comics = [comic for comic in all_comics 
                        if comic.get("ten_truyen") and comic.get("link_truyen") 
                        and comic.get("link_truyen").startswith("http")]
        
        logger.info(f"Tổng cộng đã tìm thấy {len(valid_comics)} truyện hợp lệ để crawl (từ {len(all_comics)} kết quả ban đầu)")
        return valid_comics
    
    @retry(max_retries=2)
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản của truyện từ trang TruyenQQ với multiprocessing"""
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
            
            # Lấy danh sách truyện
            raw_comics = self.get_comic_listings(self.max_pages, progress_callback)
            logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
            self.total_comics = len(raw_comics)
            
            # Nếu không lấy được truyện nào, kết thúc
            if not raw_comics:
                logger.warning("Không lấy được truyện nào, kết thúc quá trình crawl")
                return {"count": 0, "time_taken": time.time() - start_time, "website": "TruyenQQ"}
            
            # Xử lý theo batch để kiểm soát tài nguyên tốt hơn
            batch_size = min(100, len(raw_comics))
            
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
                    dynamic_worker_count = min(self.worker_count, max(1, psutil.cpu_count(logical=False) - 1))
                    
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
                                progress = 25 + (self.processed_comics.value / len(raw_comics)) * 75
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
            "website": "TruyenQQ"
        }
    
    @retry(max_retries=2)
    def crawl_comic_details(self, comic):
        """Crawl thông tin chi tiết của một truyện (phiên bản truyền thống dùng cho API)"""
        driver = None
        
        try:
            # Kiểm tra tài nguyên trước khi tạo driver
            if not check_system_resources():
                logger.warning(f"Tài nguyên hệ thống không đủ, bỏ qua truyện {comic.get('ten_truyen', '')}")
                return comic
            
            # Khởi tạo driver
            driver = create_chrome_driver()
            
            comic_url = comic["link_truyen"]
            logger.debug(f"Đang crawl chi tiết truyện: {comic_url}")
            
            for attempt in range(5):
                try:
                    driver.get(comic_url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "li.author.row p.col-xs-9 a"))
                    )
                    break
                except Exception as e:
                    logger.warning(f"Thử lần {attempt + 1}: {e}")
                    time.sleep(random.uniform(2, 4))
            else:
                logger.error("Không thể truy cập trang sau 5 lần thử")
                if driver:
                    driver.quit()
                return comic
            
            # Kiểm tra xem có phần tử tên khác không
            ten_khac_element = driver.find_elements(By.CSS_SELECTOR, "li.othername.row h2")
            if ten_khac_element:
                comic["ten_khac"] = ten_khac_element[0].text.strip()
                comic["tac_gia"] = get_text_safe(driver, "li.author.row p.col-xs-9 a")
                comic["trang_thai"] = get_text_safe(driver, "li.status.row p.col-xs-9")
                comic["luot_thich"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9.number-like")
                comic["luot_theo_doi"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
                comic["luot_xem"] = get_text_safe(driver, "li:nth-child(6) p.col-xs-9")
            else:
                comic["ten_khac"] = "Không có tên khác"
                comic["tac_gia"] = get_text_safe(driver, "li.author.row p.col-xs-9 a")
                comic["trang_thai"] = get_text_safe(driver, "li.status.row p.col-xs-9")
                comic["luot_thich"] = get_text_safe(driver, "li:nth-child(3) p.col-xs-9.number-like")
                comic["luot_theo_doi"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9")
                comic["luot_xem"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
            
            # Cố gắng nhấp vào "Xem thêm" nếu có để lấy mô tả đầy đủ
            try:
                readmore_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "p > a"))
                )
                readmore_button.click()
                time.sleep(1)
            except:
                pass
            
            # Lấy mô tả
            comic["mo_ta"] = get_text_safe(driver, "div.story-detail-info.detail-content")
            
            # Chuyển các giá trị sang số
            try:
                comic["luot_xem"] = extract_number(comic["luot_xem"])
                comic["luot_thich"] = extract_number(comic["luot_thich"])
                comic["luot_theo_doi"] = extract_number(comic["luot_theo_doi"])
            except:
                pass
                
        except Exception as e:
            logger.error(f"Lỗi khi crawl chi tiết truyện: {str(e)}")
            # Đảm bảo vẫn trả về đối tượng comic với thông tin cơ bản
        finally:
            if driver:
                driver.quit()
                
        return comic
    
    @retry(max_retries=2)
    def crawl_comments(self, comic, time_limit=None, days_limit=None):
        """Crawl comment cho một truyện cụ thể"""
        driver = None
        all_comments = []
        
        try:
            comic_url = comic.get("link_truyen")
            comic_id = comic.get("id")
            
            if not comic_url or not comic_id:
                logger.error(f"Không tìm thấy link hoặc ID truyện: {comic.get('ten_truyen', 'Unknown')}")
                if driver:
                    driver.quit()
                return []
                
            # logger.info(f"Đang crawl comment cho truyện: {comic.get('ten_truyen')} (ID: {comic_id})")
            
            # Khởi tạo WebDriver
            driver = create_chrome_driver()
            
            try:
                driver.get(comic_url)
            except Exception as e:
                logger.error(f"Lỗi khi truy cập URL {comic_url}: {str(e)}")
                if driver:
                    driver.quit()
                return []
                        
            time.sleep(random.uniform(1, 2))  
            
            # Kiểm tra xem trang có tồn tại không
            if "Page not found" in driver.title or "404" in driver.title:
                logger.error(f"Trang không tồn tại: {comic_url}")
                if driver:
                    driver.quit()
                return []
            
            # Lặp qua các trang comment
            page_comment = 1
            max_pages = 100  
            stop_crawling = False
            while page_comment <= max_pages and not stop_crawling:
                try:
                    # Debug thông tin trang hiện tại
                    logger.info(f"Đang tải comment trang {page_comment}...")
                    
                    # Kiểm tra xem có hàm loadComment không
                    has_load_comment = driver.execute_script("return typeof loadComment === 'function'")
                    if not has_load_comment:
                        logger.warning("Hàm loadComment không tồn tại trên trang")
                        break
                    
                    # Gọi hàm loadComment để tải comment trang tiếp theo
                    try:
                        driver.execute_script("loadComment(arguments[0]);", page_comment)
                    except Exception as e:
                        logger.error(f"Lỗi khi gọi hàm loadComment: {str(e)}")
                        break
                        
                    time.sleep(random.uniform(2, 3))  
                    
                    # Kiểm tra xem có comment không
                    comment_elements = driver.find_elements(By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment")
                    
                    if not comment_elements:
                        logger.info(f"Không tìm thấy comment nào trên trang {page_comment}")
                        break
                        
                    logger.info(f"Tìm thấy {len(comment_elements)} comment trên trang {page_comment}")
                    
                    # Xử lý từng comment
                    new_comments_found = 0
                    old_comments_count = 0
                    for comment_elem in comment_elements:
                        try:
                            # Thử dùng JavaScript để lấy thông tin
                            name = driver.execute_script("""
                                return arguments[0].querySelector('div.outsite-comment div.outline-content-comment div:nth-child(1) strong')?.innerText || "N/A";
                            """, comment_elem)
                            
                            content = driver.execute_script("""
                                return arguments[0].querySelector('div.outsite-comment div.outline-content-comment div.content-comment')?.innerText || "N/A";
                            """, comment_elem)
                            
                            time_text = driver.execute_script("""
                                return arguments[0].querySelector('div.action-comment span.time')?.innerText || "N/A";
                            """, comment_elem)

                            # Đảm bảo nội dung bình luận không để trống
                            if not content or content.strip() == "":
                                content = "N/A"
                                
                            # Đảm bảo tên người bình luận không để trống
                            if not name or name.strip() == "":
                                name = "N/A"
                                
                            comment_time = datetime.now()  # Mặc định là thời gian hiện tại
                            if time_text:
                                comment_time = parse_relative_time(time_text)
                            
                            if time_limit and comment_time < time_limit:
                                logger.debug(f"Comment quá cũ ({time_text}), bỏ qua")
                                old_comments_count += 1
                                continue
                            
                            # Tạo đối tượng comment
                            comment_data = {
                                "ten_nguoi_binh_luan": name,
                                "noi_dung": content,
                                "comic_id": comic_id,
                                "thoi_gian_binh_luan": comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                                "thoi_gian_cap_nhat": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            
                            # Kiểm tra trùng lặp trước khi thêm
                            is_duplicate = False
                            for existing in all_comments:
                                if (existing["ten_nguoi_binh_luan"] == name and 
                                    existing["noi_dung"] == content):
                                    is_duplicate = True
                                    break
                            
                            if not is_duplicate:
                                all_comments.append(comment_data)
                                new_comments_found += 1
                                
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý comment: {str(e)}")
                    
                    logger.info(f"Đã thêm {new_comments_found} comment mới từ trang {page_comment}")
                    
                    if time_limit and old_comments_count > 0 and old_comments_count == len(comment_elements):
                        logger.info(f"Tất cả {len(comment_elements)} comment trên trang {page_comment} đều cũ hơn {days_limit} ngày, dừng crawl")
                        stop_crawling = True
                        break                    
                    
                    # Nếu không có comment mới nào được thêm, dừng lại
                    if new_comments_found == 0:
                        logger.info(f"Không tìm thấy comment mới nào trên trang {page_comment}")
                        break
                    
                    # Chuyển đến trang tiếp theo
                    page_comment += 1
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý trang comment {page_comment}: {str(e)}")
                    break
            
            # Lưu comments vào database sử dụng SQLiteHelper
            if all_comments:
                logger.info(f"Lưu {len(all_comments)} comment cho truyện ID {comic_id}")
                self.sqlite_helper.save_comments_to_db(comic_id, all_comments, "TruyenQQ")
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {str(e)}")
            all_comments = []
            
        finally:
            if driver is not None:
                driver.quit()
        return all_comments

# Import cần thiết, thêm vào phần đầu nếu cần
import sys
import traceback

# Đặt exception hook để ghi log lỗi không được xử lý
def global_exception_handler(exctype, value, tb):
    logger.critical("Lỗi không bắt được: %s", ''.join(traceback.format_exception(exctype, value, tb)))
    sys.__excepthook__(exctype, value, tb)

sys.excepthook = global_exception_handler