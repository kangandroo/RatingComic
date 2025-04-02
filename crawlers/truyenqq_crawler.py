from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
import re
import os
import queue
import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
from utils.sqlite_helper import SQLiteHelper

logger = logging.getLogger(__name__)

class DriverPool:
    """Quản lý pool của WebDriver để tái sử dụng giữa các threads"""
    
    def __init__(self, max_drivers=25, setup_func=None):
        self.drivers = queue.Queue()
        self.max_drivers = max_drivers
        self.setup_func = setup_func
        self.active_drivers = 0
        self.lock = threading.Lock()
        self.all_drivers = []  # Để theo dõi tất cả driver đã tạo
    
    def get_driver(self):
        """Lấy driver từ pool hoặc tạo mới nếu cần"""
        try:
            # Thử lấy driver có sẵn từ pool
            return self.drivers.get_nowait()
        except queue.Empty:
            # Nếu pool rỗng và chưa đạt giới hạn, tạo driver mới
            with self.lock:
                if self.active_drivers < self.max_drivers:
                    driver = self.setup_func()
                    self.active_drivers += 1
                    self.all_drivers.append(driver)
                    return driver
                else:
                    # Đã đạt giới hạn, đợi 1 driver được trả về
                    logger.info("Đã đạt giới hạn driver, đợi driver được trả về")
                    return self.drivers.get()
    
    def return_driver(self, driver):
        """Trả driver về pool để tái sử dụng"""
        if driver:
            # Xóa sạch cookies trước khi tái sử dụng
            try:
                driver.delete_all_cookies()
            except:
                pass
            self.drivers.put(driver)
    
    def close_all(self):
        """Đóng tất cả driver khi hoàn thành"""
        logger.info(f"Đóng tất cả {len(self.all_drivers)} driver...")
        for driver in self.all_drivers:
            try:
                driver.quit()
            except:
                pass
        self.all_drivers.clear()
        self.active_drivers = 0
        
        # Xóa queue
        while not self.drivers.empty():
            try:
                self.drivers.get_nowait()
            except:
                break

class TruyenQQCrawler(BaseCrawler):
    """Crawler cho trang TruyenQQ dựa trên code mẫu đã được tối ưu"""
    
    def __init__(self, db_manager, config_manager, base_url=None, max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        
        # Đặt base_url từ tham số hoặc giá trị mặc định
        self.base_url = base_url if base_url else "https://truyenqqto.com"
        self.max_pages = max_pages
        self.worker_count = worker_count
        
        # Khởi tạo SQLiteHelper
        self.sqlite_helper = SQLiteHelper(self.db_manager.db_folder)
        
        # User agent để tránh bị chặn
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        
        # Tạo một thread local storage để lưu kết nối SQLite cho mỗi thread
        self.thread_local = threading.local()
        
        logger.info(f"Khởi tạo TruyenQQCrawler với base_url={self.base_url}")
    
    def create_chrome_driver(self):
        """Tạo và cấu hình Chrome WebDriver với các tùy chọn vô hiệu hóa TensorFlow và GPU"""
        chrome_options = Options()
        
        # Cấu hình cơ bản
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        # === VÔ HIỆU HÓA TENSORFLOW & ML ===
        chrome_options.add_argument("--disable-features=BlinkGenPropertyTrees,AcceleratedSmallCanvases,CanvasHitRegions,CanvasImageSmoothing")
        chrome_options.add_argument("--disable-machine-learning")
        chrome_options.add_argument("--disable-blink-features=NativeFileSystemAPI")
        
        # Vô hiệu hóa TensorFlow Lite và các tính năng ML
        chrome_options.add_argument("--disable-features=TensorFlowLite,TensorFlowLiteServerSide,TensorFlowLiteEngine,TensorFlowLiteEmbedded,TensorFlowLiteGPU")
        chrome_options.add_argument("--disable-features=NeuralNetworkTensorflowEstimator,NeuralNetworkModelLoader")
        chrome_options.add_argument("--disable-features=OptimizationHints,DocumentInFlightNetworkHints")
        
        # === VÔ HIỆU HÓA GPU & HARDWARE ACCELERATION ===
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-webgl")
        chrome_options.add_argument("--disable-webgl2")
        chrome_options.add_argument("--disable-accelerated-2d-canvas")
        chrome_options.add_argument("--disable-3d-apis")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--use-gl=swiftshader")
        chrome_options.add_argument("--use-angle=swiftshader")
        
        # Vô hiệu hóa các tính năng đồ họa nâng cao
        chrome_options.add_argument("--disable-canvas-aa")
        chrome_options.add_argument("--disable-2d-canvas-clip-aa")
        chrome_options.add_argument("--disable-gl-drawing-for-tests")
        chrome_options.add_argument("--force-color-profile=srgb")
        
        # === TỐI ƯU CHO ĐA LUỒNG ===
        chrome_options.add_argument("--single-process")  # Quan trọng cho đa luồng
        chrome_options.add_argument("--memory-model=low")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--window-size=1280,1024")
        chrome_options.add_argument("--disable-notifications")
        
        # Tắt các tính năng tiêu tốn tài nguyên
        chrome_options.add_argument("--disable-remote-fonts")
        chrome_options.add_argument("--disable-features=LazyFrameLoading,BlinkRuntimeCallStats")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-background-networking")
        
        # Tắt TensorFlow logging hoàn toàn
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
        os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
        os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning,ignore::UserWarning'
        
        prefs = {
            # Tắt tải hình ảnh
            'profile.default_content_settings.images': 2,
            'profile.managed_default_content_settings.images': 2,
            
            'profile.default_content_settings.cookies': 2,
            'profile.managed_default_content_settings.cookies': 2,
            
            'plugins.plugins_disabled': ['Chrome PDF Viewer'],
            
            'profile.default_content_settings.storage': 2,
            'profile.managed_default_content_settings.storage': 2,
            
            'profile.default_content_settings.notifications': 2,
            'profile.default_content_settings.geolocation': 2,
            
            'webgl.disabled': True,
            'hardware_acceleration_mode.enabled': False,
            
            'profile.hardware_acceleration_enabled': False,
        }
        chrome_options.add_experimental_option('prefs', prefs)
        
        # Tắt logging và automation flags
        chrome_options.add_experimental_option('excludeSwitches', ["enable-automation", "enable-logging"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            # Lấy đường dẫn đến ChromeDriver
            chromedriver_path = self.config_manager.get_chrome_driver_path()
            
            if chromedriver_path and os.path.exists(chromedriver_path):
                logger.info(f"Sử dụng ChromeDriver từ: {chromedriver_path}")
                service = Service(chromedriver_path)
                service.log_path = os.devnull  # Vô hiệu hóa Selenium log
                return webdriver.Chrome(service=service, options=chrome_options)
            else:
                service = Service(log_path=os.devnull)  # Vô hiệu hóa Selenium log
                return webdriver.Chrome(options=chrome_options, service=service)
                
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Chrome driver: {e}")
            try:
                # Fallback mode - thử một lần nữa với ít tùy chọn hơn
                fallback_options = Options()
                fallback_options.add_argument("--headless")
                fallback_options.add_argument("--no-sandbox")
                fallback_options.add_argument("--disable-gpu")
                fallback_options.add_argument("--disable-dev-shm-usage")
                return webdriver.Chrome(options=fallback_options)
            except Exception as e2:
                logger.critical(f"Lỗi nghiêm trọng khi khởi tạo Chrome driver: {e2}")
                raise RuntimeError(f"Không thể khởi tạo Chrome driver: {e2}")
            
    def get_text_safe(self, element, selector):
        """Trích xuất nội dung văn bản an toàn từ một phần tử sử dụng bộ chọn CSS"""
        try:
            return element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except:
            return "N/A"
            
    def extract_chapter_number(self, chapter_text):
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
    
    def get_comic_listings(self, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ các trang danh sách"""
        all_comics = []
        page_num = 1
        driver = None
        
        try:
            # Số trang tối đa
            max_pages = max_pages if max_pages else self.max_pages if self.max_pages else 999
            
            # Khởi tạo driver riêng cho việc thu thập danh sách truyện
            driver = self.create_chrome_driver()
            
            # Duyệt qua từng trang
            while page_num <= max_pages:
                try:
                    url = f"{self.base_url}/truyen-moi-cap-nhat/trang-{page_num}.html"
                    logger.info(f"Đang crawl trang {page_num}: {url}")
                    
                    driver.get(url)
                    time.sleep(random.uniform(1, 2))
                    
                    # Chờ và kiểm tra xem có truyện nào trên trang không
                    try:
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".book_name.qtip h3 a"))
                        )
                        
                        # Lấy danh sách các khối truyện
                        story_blocks = driver.find_elements(By.CSS_SELECTOR, ".list_grid_out ul.list_grid li")
                        
                        # Nếu không tìm thấy truyện nào, thoát khỏi vòng lặp
                        if not story_blocks:
                            logger.info(f"Không tìm thấy truyện nào ở trang {page_num}. Có thể đã đến trang cuối cùng.")
                            break
                        
                        # Thu thập dữ liệu từ trang hiện tại
                        page_stories = []
                        
                        for story_block in story_blocks:
                            # Lấy thông tin tên và link truyện
                            try:
                                name_elem = story_block.find_element(By.CSS_SELECTOR, ".book_name.qtip h3 a")
                                story_name = name_elem.get_attribute("title")
                                story_link = name_elem.get_attribute("href")
                            except Exception:
                                continue
                            
                            # Lấy thông tin chương
                            chapter_info = "Chapter 0"  # Giá trị mặc định
                            try:
                                chapter_elements = story_block.find_elements(By.CSS_SELECTOR, ".last_chapter a")
                                if chapter_elements:
                                    chapter_info = chapter_elements[0].get_attribute("title") or "Chapter 0"
                            except Exception:
                                pass
                            
                            # Trích xuất số chương
                            chapter_count = self.extract_chapter_number(chapter_info)
                            
                            # Tạo đối tượng truyện
                            comic_data = {
                                "ten_truyen": story_name,
                                "link_truyen": story_link,
                                "so_chuong": chapter_count,
                                "nguon": "TruyenQQ"
                            }
                            
                            page_stories.append(comic_data)
                        
                        logger.info(f"Trang {page_num}: Đã tìm thấy {len(page_stories)} truyện")
                        all_comics.extend(page_stories)
                        
                        # Chuyển sang trang tiếp theo
                        page_num += 1
                        
                        # Cập nhật tiến trình
                        if progress_callback:
                            progress = min(25, (page_num / max_pages) * 25)  # Giới hạn 25% cho giai đoạn crawl danh sách
                            progress_callback.emit(int(progress))
                        
                    except Exception as e:
                        logger.warning(f"Không thể lấy dữ liệu từ trang {page_num}: {str(e)}")
                        break  
                        
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập trang {page_num}: {str(e)}")
                    break  
                    
                # Nghỉ ngẫu nhiên giữa các yêu cầu để tránh bị chặn
                time.sleep(random.uniform(1, 3))
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {str(e)}")
            
        finally:
            # Đóng driver sau khi sử dụng xong
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                    
        logger.info(f"Tổng cộng đã tìm thấy {len(all_comics)} truyện")
        return all_comics
    
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản của truyện từ trang TruyenQQ với cơ chế batch và pool driver"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Khởi tạo driver pool
            driver_pool = DriverPool(max_drivers=self.worker_count, setup_func=self.create_chrome_driver)
            
            # Lấy danh sách truyện (sử dụng hàm riêng không dùng pool)
            raw_comics = self.get_comic_listings(self.max_pages, progress_callback)
            logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
            
            # Biến theo dõi số lượng comics đã xử lý
            processed_count = 0
            
            # Cấu trúc dữ liệu để theo dõi tiến độ
            data_lock = threading.Lock()
            batch_size = min(100, len(raw_comics)) 
            
            # Hàm xử lý một comic
            def process_comic(comic, driver_pool, batch_stopping):
                nonlocal processed_count
                
                if batch_stopping.is_set():
                    return None
                
                # Lấy driver từ pool
                driver = driver_pool.get_driver()
                
                try:
                    # Lấy thông tin chi tiết
                    detailed_comic = self.crawl_comic_details_with_driver(comic, driver)
                    
                    if detailed_comic:
                        # Lưu vào database
                        self.sqlite_helper.save_comic_to_db(detailed_comic, "TruyenQQ")
                        
                        # Cập nhật counter an toàn
                        with data_lock:
                            processed_count += 1
                            
                            # Cập nhật tiến độ
                            if progress_callback and len(raw_comics) > 0:
                                progress = 25 + (processed_count / len(raw_comics)) * 75
                                progress_callback.emit(int(min(progress, 100)))
                                
                    return detailed_comic
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý truyện {comic.get('ten_truyen', '')}: {e}")
                    return None
                finally:
                    # Trả driver lại pool
                    driver_pool.return_driver(driver)
            
            # Xử lý theo batch để kiểm soát tài nguyên tốt hơn
            for i in range(0, len(raw_comics), batch_size):
                # Mỗi batch có một flag batch_stopping riêng
                batch_stopping = threading.Event()
                
                batch = raw_comics[i:i+batch_size]
                logger.info(f"Xử lý batch {i//batch_size + 1}/{(len(raw_comics)-1)//batch_size + 1} ({len(batch)} truyện)")
                
                batch_comics_count = 0
                
                # Tạo executor mới cho mỗi batch
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_count) as executor:
                    futures = {executor.submit(process_comic, comic, driver_pool, batch_stopping): comic for comic in batch}
                    
                    try:
                        # Đợi các future hoàn thành với timeout
                        for future in concurrent.futures.as_completed(futures, timeout=180):
                            comic = futures[future]
                            try:
                                result = future.result()
                                if result:
                                    comics_count += 1
                                    batch_comics_count += 1
                                    logger.info(f"Hoàn thành {comics_count}/{len(raw_comics)}: {comic.get('ten_truyen', '')}")
                            except Exception as e:
                                logger.error(f"Lỗi khi xử lý truyện {comic.get('ten_truyen', '')}: {e}")
                    except concurrent.futures.TimeoutError:
                        # Đếm số lượng tasks hoàn thành và chưa hoàn thành
                        completed = sum(1 for f in futures if f.done())
                        pending = sum(1 for f in futures if not f.done())
                        
                        logger.warning(f"Timeout khi xử lý batch. Đã xử lý {completed}/{len(futures)} tasks trong batch hiện tại. Còn {pending} tasks đang chờ.")
                        logger.warning(f"Tổng số truyện đã xử lý: {comics_count}")
                        
                        batch_stopping.set()  # Chỉ dừng batch hiện tại
                        
                        # Hủy các future đang chạy
                        for future in futures:
                            if not future.done():
                                future.cancel()
                        
                        logger.warning(f"Đã hủy batch {i//batch_size + 1} do timeout. Chuyển sang batch tiếp theo.")
                    
                    logger.info(f"Kết thúc batch {i//batch_size + 1}: Đã xử lý {batch_comics_count}/{len(batch)} truyện trong batch")
                
                # Gọi garbage collector
                import gc
                gc.collect()
                
                # Pause nhỏ giữa các batch để giải phóng tài nguyên
                logger.info("Nghỉ giữa các batch để giải phóng tài nguyên...")
                time.sleep(3)
                    
        except Exception as e:
            logger.error(f"Lỗi trong quá trình crawl: {e}")
        finally:
            # Đóng tất cả driver
            if 'driver_pool' in locals():
                driver_pool.close_all()
            
            # Đóng tất cả kết nối SQLite trong thread hiện tại
            self.sqlite_helper.close_all_connections()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Đã crawl {comics_count} truyện trong {elapsed_time:.2f} giây")
        
        return {
            "count": comics_count,
            "time_taken": time.time() - start_time,
            "website": "TruyenQQ"
        }
    
    def crawl_comic_details_with_driver(self, comic, driver):
        """Crawl thông tin chi tiết của một truyện với driver được cung cấp từ pool"""
        try:
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
                    logger.warning(f"Thử lần {attempt + 1}")
                    time.sleep(random.uniform(2, 4))
            else:
                logger.error("Không thể truy cập trang sau 5 lần thử")
                return comic
            
            # Kiểm tra xem có phần tử tên khác không
            ten_khac_element = driver.find_elements(By.CSS_SELECTOR, "li.othername.row h2")
            if ten_khac_element:
                comic["ten_khac"] = ten_khac_element[0].text.strip()
                comic["tac_gia"] = self.get_text_safe(driver, "li.author.row p.col-xs-9 a")
                comic["trang_thai"] = self.get_text_safe(driver, "li.status.row p.col-xs-9")
                comic["luot_thich"] = self.get_text_safe(driver, "li:nth-child(4) p.col-xs-9.number-like")
                comic["luot_theo_doi"] = self.get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
                comic["luot_xem"] = self.get_text_safe(driver, "li:nth-child(6) p.col-xs-9")
            else:
                comic["ten_khac"] = "Không có tên khác"
                comic["tac_gia"] = self.get_text_safe(driver, "li.author.row p.col-xs-9 a")
                comic["trang_thai"] = self.get_text_safe(driver, "li.status.row p.col-xs-9")
                comic["luot_thich"] = self.get_text_safe(driver, "li:nth-child(3) p.col-xs-9.number-like")
                comic["luot_theo_doi"] = self.get_text_safe(driver, "li:nth-child(4) p.col-xs-9")
                comic["luot_xem"] = self.get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
            
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
            comic["mo_ta"] = self.get_text_safe(driver, "div.story-detail-info.detail-content")
            
            # Chuyển các giá trị sang số
            try:
                comic["luot_xem"] = self.extract_number(comic["luot_xem"])
                comic["luot_thich"] = self.extract_number(comic["luot_thich"])
                comic["luot_theo_doi"] = self.extract_number(comic["luot_theo_doi"])
            except:
                pass  
        except Exception as e:
            logger.error(f"Lỗi khi crawl chi tiết truyện: {str(e)}")
            # Đảm bảo vẫn trả về đối tượng comic với thông tin cơ bản
        
        return comic
    
    def crawl_comic_details(self, comic):
        """Crawl thông tin chi tiết của một truyện (phiên bản truyền thống dùng cho API)"""
        driver = self.create_chrome_driver()
        
        try:
            result = self.crawl_comic_details_with_driver(comic, driver)
        finally:
            if driver:
                driver.quit()
        return result
    
    def crawl_comments(self, comic):
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
            driver = self.create_chrome_driver()
            
            try:
                driver.get(comic_url)
            except Exception as e:
                logger.error(f"Lỗi khi truy cập URL {comic_url}: {str(e)}")
                if driver:
                    driver.quit()
                return []
                        
            time.sleep(random.uniform(2, 3))  # Tăng thời gian chờ
            
            # Kiểm tra xem trang có tồn tại không
            if "Page not found" in driver.title or "404" in driver.title:
                logger.error(f"Trang không tồn tại: {comic_url}")
                if driver:
                    driver.quit()
                return []
            
            # Lặp qua các trang comment
            page_comment = 1
            max_pages = 100  
            while page_comment <= max_pages:
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
                        
                    time.sleep(random.uniform(2, 3))  # Tăng thời gian chờ
                    
                    # Kiểm tra xem có comment không
                    comment_elements = driver.find_elements(By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment")
                    
                    if not comment_elements:
                        logger.info(f"Không tìm thấy comment nào trên trang {page_comment}")
                        break
                        
                    logger.info(f"Tìm thấy {len(comment_elements)} comment trên trang {page_comment}")
                    
                    # Xử lý từng comment
                    new_comments_found = 0
                    for comment_elem in comment_elements:
                        try:
                            # Thử dùng JavaScript để lấy thông tin
                            name = driver.execute_script("""
                                return arguments[0].querySelector('div.outsite-comment div.outline-content-comment div:nth-child(1) strong')?.innerText || "N/A";
                            """, comment_elem)
                            
                            content = driver.execute_script("""
                                return arguments[0].querySelector('div.outsite-comment div.outline-content-comment div.content-comment')?.innerText || "N/A";
                            """, comment_elem)
                            
                            # Đảm bảo nội dung bình luận không để trống
                            if not content or content.strip() == "":
                                content = "N/A"
                                
                            # Đảm bảo tên người bình luận không để trống
                            if not name or name.strip() == "":
                                name = "N/A"
                            
                            # Tạo đối tượng comment
                            comment_data = {
                                "ten_nguoi_binh_luan": name,
                                "noi_dung": content,
                                "comic_id": comic_id
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
    
    def extract_number(self, text_value):
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