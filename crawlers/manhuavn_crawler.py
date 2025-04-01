from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
import re
import os
import queue
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
import threading
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

class ManhuavnCrawler(BaseCrawler):
    """Crawler cho trang Manhuavn"""
    
    def __init__(self, db_manager, config_manager, base_url="https://manhuavn.top", max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        
        # Đặt base_url từ tham số hoặc giá trị mặc định
        self.base_url = base_url if base_url else "https://manhuavn.top"
        self.max_pages = max_pages
        self.worker_count = worker_count
        
        # Khởi tạo SQLiteHelper
        self.sqlite_helper = SQLiteHelper(self.db_manager.db_folder)
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
        
        # Tạo một thread local storage để lưu kết nối SQLite cho mỗi thread
        self.thread_local = threading.local()
        
        # logger.info(f"Khởi tạo ManhuavnCrawler với base_url={self.base_url}")
        
    def setup_driver(self):
        """Cấu hình Chrome WebDriver tối ưu cho crawl đa luồng"""
        chrome_options = Options()
        
        # Chạy chế độ headless nếu không cần giao diện
        chrome_options.add_argument("--headless")  
        
        # Tắt các tính năng không cần thiết
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-features=NeuralNetworkTensorflowEstimator,OptimizationHints")
        chrome_options.add_argument("--disable-accelerated-2d-canvas")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-features")

        # Giảm tài nguyên tiêu thụ
        chrome_options.add_argument("--single-process")
        chrome_options.add_argument("--memory-model=low")
        chrome_options.add_argument("--window-size=1280,1024")
        
        # Bỏ qua cảnh báo automation
        chrome_options.add_experimental_option('excludeSwitches', ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Vô hiệu hóa logging để tránh crash
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
        os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
        os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
        os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning,ignore::UserWarning'

        try:
            service = Service(log_path=os.devnull)  # Tắt log của Selenium
            return webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            raise RuntimeError(f"Lỗi khi khởi tạo Chrome driver: {e}")
    
    def get_text_safe(self, element, selector):
        """Trích xuất nội dung văn bản an toàn từ phần tử"""
        try:
            return element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except Exception:
            return "N/A"
    
    def parse_number(self, text):
        """Chuyển đổi các số có đơn vị K, M thành số nguyên."""
        if not text or text == "N/A":
            return 0

        text = text.lower().strip()
        multiplier = 1

        if "k" in text:
            multiplier = 1000
            text = text.replace("k", "")
        elif "m" in text:
            multiplier = 1000000
            text = text.replace("m", "")

        try:
            return int(float(text) * multiplier)
        except ValueError:
            return 0
    
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản từ Manhuavn với pool driver"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Khởi tạo driver pool
            driver_pool = DriverPool(max_drivers=self.worker_count, setup_func=self.setup_driver)
            
            # Lấy danh sách truyện (sử dụng driver riêng)
            driver = self.setup_driver()
            try:
                raw_comics = self.get_all_stories(driver, self.max_pages, progress_callback)
                logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
            finally:
                driver.quit()
            
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
                    detailed_comic = self.get_story_details(comic, driver)
                    
                    if detailed_comic:
                        # Chuyển đổi định dạng
                        db_comic = self.transform_comic_data(detailed_comic)
                        
                        # Lưu vào database
                        self.sqlite_helper.save_comic_to_db(db_comic, "Manhuavn")
                        
                        # Cập nhật counter an toàn
                        with data_lock:
                            processed_count += 1
                            
                            # Cập nhật tiến độ
                            if progress_callback and len(raw_comics) > 0:
                                progress = (processed_count / len(raw_comics)) * 100
                                progress_callback.emit(int(progress))
                                
                    return detailed_comic
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
                    return None
                finally:
                    # Trả driver lại pool
                    driver_pool.return_driver(driver)
            
            # Xử lý theo batch để kiểm soát tài nguyên tốt hơn
            for i in range(0, len(raw_comics), batch_size):
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
                                    logger.info(f"Hoàn thành {comics_count}/{len(raw_comics)}: {comic.get('Tên truyện', '')}")
                            except Exception as e:
                                logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
                    except concurrent.futures.TimeoutError:
                        logger.warning(f"Timeout khi xử lý batch. Đã xử lý {comics_count} truyện.")
                        batch_stopping.set()  
                        
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
            "website": "Manhuavn"
        }
    
    def get_all_stories(self, driver, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        stories = []
        page = 1
        
        try:
            while max_pages is None or page <= max_pages:
                url = f"{self.base_url}/danhsach/P{page}/index.html?status=0&sort=2"
                logger.info(f"Đang tải trang {page}: {url}")
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".lst_story .story_item"))
                    )
                except Exception:
                    logger.info(f"Không tìm thấy phần tử truyện trên trang {page}, kết thúc")
                    break

                # Lấy tất cả các item truyện
                item_elements = driver.find_elements(By.CSS_SELECTOR, ".lst_story .story_item")
                
                if not item_elements:
                    logger.info("Không tìm thấy truyện nào trên trang, kết thúc")
                    break
                    
                for item in item_elements:
                    try:
                        # Lấy tiêu đề và link truyện
                        title = self.get_text_safe(item, ".story_title")
                        link = item.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                        
                        if link:
                            stories.append({
                                "Tên truyện": title, 
                                "Link truyện": link
                            })
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý truyện: {e}")
                        continue

                # Cập nhật tiến độ
                if progress_callback and max_pages:
                    progress = min(25, (page / max_pages) * 25)  # Chỉ chiếm 25% đầu tiên
                    progress_callback.emit(int(progress))

                page += 1
                time.sleep(random.uniform(2, 3))
                
        except Exception as e:
            logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
            
        logger.info(f"Đã tìm thấy {len(stories)} truyện để crawl")
        return stories
        
    def get_story_details(self, story, driver):
        """Lấy thông tin chi tiết của truyện"""
        try:
            for attempt in range(5):
                try:
                    driver.get(story["Link truyện"])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".info-row .contiep"))
                    )
                    break
                except Exception as e:
                    logger.warning(f"Thử lần {attempt + 1}")
                    time.sleep(random.uniform(2, 4))
            else:
                logger.error("Không thể truy cập trang sau 5 lần thử")
                return None

            # Lấy thông tin chi tiết
            story["Tình trạng"] = self.get_text_safe(driver, ".info-row .contiep")
            story["Lượt theo dõi"] = self.get_text_safe(driver, "li.info-row strong")
            story["Lượt xem"] = self.parse_number(self.get_text_safe(driver, "li.info-row view.colorblue"))
            story["Đánh giá"] = self.get_text_safe(driver, 'span[itemprop="ratingValue"]')
            story["Lượt đánh giá"] = self.get_text_safe(driver, 'span[itemprop="ratingCount"]')
            story["Mô tả"] = self.get_text_safe(driver, "li.clearfix p")

            # Lấy số chương
            chapter_text = self.get_text_safe(driver, "li.info-row a.colorblue")
            chapter_match = re.search(r'\d+', chapter_text)
            story["Số chương"] = chapter_match.group() if chapter_text != "N/A" and chapter_match else "0"
            
            # Lấy tác giả
            try:
                author_element = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/ul/li[6]/a")
                story["Tác giả"] = author_element.text.strip()
            except:
                story["Tác giả"] = "N/A"

        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin chi tiết truyện {story.get('Tên truyện')}: {e}")
            return None
            
        return story
    
    def transform_comic_data(self, raw_comic):
        """Chuyển đổi dữ liệu raw sang định dạng database"""
        return {
            "ten_truyen": raw_comic.get("Tên truyện", ""),
            "tac_gia": raw_comic.get("Tác giả", "N/A"),
            "mo_ta": raw_comic.get("Mô tả", ""),
            "link_truyen": raw_comic.get("Link truyện", ""),
            "so_chuong": int(raw_comic.get("Số chương", "0")) if raw_comic.get("Số chương", "0").isdigit() else 0,
            "luot_xem": raw_comic.get("Lượt xem", "0"),
            "luot_theo_doi": self.extract_number(raw_comic.get("Lượt theo dõi", "0")),
            "danh_gia": raw_comic.get("Đánh giá", "0"),
            "luot_danh_gia": self.extract_number(raw_comic.get("Lượt đánh giá", "0")),
            "trang_thai": raw_comic.get("Tình trạng", ""),
            "nguon": "Manhuavn"
        }
    
    def crawl_comments(self, comic):
        """Crawl comment cho một truyện cụ thể"""
        driver = self.setup_driver()
        comments = []
        
        try:
            # Lấy link từ comic
            link = comic.get("link_truyen")
            # logger.info(f"Link truyện: {link}")
            comic_id = comic.get("id")
            
            if not link or not comic_id:
                logger.error(f"Không tìm thấy link hoặc ID truyện: {comic.get('ten_truyen', 'Unknown')}")
                driver.quit()
                return []
            
            logger.info(f"Đang crawl comment cho truyện: {comic.get('ten_truyen')}")
            
            try:
                driver.get(link)
            except Exception as e:
                logger.error(f"Lỗi khi truy cập URL {link}: {str(e)}")
                driver.quit()
                return []
            
            time.sleep(random.uniform(2, 3))

            # Mở tất cả bình luận
            while True:
                try:
                    load_more_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button")
                    load_more_button.click()
                    # logger.info("Đang tải thêm bình luận...")
                    time.sleep(2)
                except:
                    break

            # Lấy danh sách bình luận
            comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment_item")
            logger.info(f"Đã tìm thấy {len(comment_elements)} comment")
            
            for comment_elem in comment_elements:
                user = self.get_text_safe(comment_elem, ".comment-head")
                content = self.get_text_safe(comment_elem, ".comment-content")
                
                # Đảm bảo nội dung bình luận không để trống
                if not content or content.strip() == "":
                    content = "N/A"
                    
                # Đảm bảo tên người bình luận không để trống
                if not user or user.strip() == "":
                    user = "Người dùng ẩn danh"
                
                # Thêm comment vào danh sách kết quả - chuyển đổi tên trường
                comments.append({
                    "ten_nguoi_binh_luan": user,
                    "noi_dung": content,
                    "comic_id": comic_id
                })
            
            # Lưu comments vào database sử dụng SQLiteHelper
            if comments:
                logger.info(f"Lưu {len(comments)} comment cho truyện ID {comic_id}")
                self.sqlite_helper.save_comments_to_db(comic_id, comments, "Manhuavn")
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {e}")
        finally:
            driver.quit()
            
        logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')}")
        return comments
    
    def extract_number(self, text_value):
        """Trích xuất số từ các chuỗi với nhiều định dạng"""
        if not text_value or text_value == "N/A":
            return 0
            
        # Chỉ lấy phần số từ chuỗi
        try:
            number_match = re.search(r'\d+', text_value)
            if number_match:
                return int(number_match.group())
            return 0
        except Exception:
            return 0