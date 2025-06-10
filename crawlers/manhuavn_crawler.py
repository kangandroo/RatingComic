from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
from datetime import datetime, timedelta
import re
import os
import multiprocessing
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils.sqlite_helper import SQLiteHelper

logger = logging.getLogger(__name__)

# Hàm khởi tạo riêng cho mỗi process
def init_process():
    """Khởi tạo các thiết lập cho mỗi process"""
    multiprocessing.current_process().daemon = False

# Định nghĩa hàm xử lý truyện ở cấp độ module
def process_comic_worker(params):
    """Hàm để xử lý một truyện trong một process riêng biệt"""
    comic, db_path, base_url = params
    
    # Tạo driver mới cho mỗi process
    driver = setup_driver()
    
    # Khởi tạo SQLiteHelper trong mỗi process
    sqlite_helper = SQLiteHelper(db_path)
    
    try:
        # Lấy chi tiết truyện
        try:
            for attempt in range(5):
                try:
                    driver.get(comic["Link truyện"])
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".info-row .contiep"))
                    )
                    break
                except Exception as e:
                    logger.warning(f"Thử lần {attempt + 1}")
                    time.sleep(random.uniform(2, 4))
            else:
                logger.error("Không thể truy cập trang sau 5 lần thử")
                driver.quit()
                return None

            # Lấy thông tin chi tiết
            story = {}
            story["Tên truyện"] = comic["Tên truyện"]
            story["Link truyện"] = comic["Link truyện"]
            
            # Lấy thông tin chi tiết
            story["Tình trạng"] = get_text_safe(driver, ".info-row .contiep")
            story["Lượt theo dõi"] = get_text_safe(driver, "li.info-row strong")
            story["Lượt xem"] = parse_number(get_text_safe(driver, "li.info-row view.colorblue"))
            story["Đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingValue"]')
            story["Lượt đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingCount"]')
            story["Mô tả"] = get_text_safe(driver, "li.clearfix p")

            # Lấy số chương
            chapter_text = get_text_safe(driver, "li.info-row a.colorblue")
            chapter_match = re.search(r'\d+', chapter_text)
            story["Số chương"] = chapter_match.group() if chapter_text != "N/A" and chapter_match else "0"
            
            # Lấy tác giả
            try:
                author_element = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/ul/li[6]/a")
                story["Tác giả"] = author_element.text.strip()
            except:
                story["Tác giả"] = "N/A"
                
            # Chuyển đổi sang định dạng database
            db_comic = {
                "ten_truyen": story.get("Tên truyện", ""),
                "tac_gia": story.get("Tác giả", "N/A"),
                "mo_ta": story.get("Mô tả", ""),
                "link_truyen": story.get("Link truyện", ""),
                "so_chuong": int(story.get("Số chương", "0")) if story.get("Số chương", "0").isdigit() else 0,
                "luot_xem": story.get("Lượt xem", "0"),
                "luot_theo_doi": extract_number(story.get("Lượt theo dõi", "0")),
                "danh_gia": story.get("Đánh giá", "0"),
                "luot_danh_gia": extract_number(story.get("Lượt đánh giá", "0")),
                "trang_thai": story.get("Tình trạng", ""),
                "nguon": "Manhuavn"
            }
            
            # Lưu vào database
            sqlite_helper.save_comic_to_db(db_comic, "Manhuavn")
            
            logger.info(f"Hoàn thành: {comic.get('Tên truyện', '')}")
            
            driver.quit()
            return db_comic
            
        except Exception as e:
            logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
            driver.quit()
            return None
            
    except Exception as e:
        logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
        if driver:
            driver.quit()
        return None

# Các hàm trợ giúp định nghĩa ở cấp module
def get_text_safe(element, selector):
    """Trích xuất nội dung văn bản an toàn từ phần tử"""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return "N/A"

def parse_number(text):
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

def extract_number(text_value):
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

def setup_driver():
    """Cấu hình Chrome WebDriver"""
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
    
    # Bỏ qua cảnh báo automation
    chrome_options.add_experimental_option('excludeSwitches', ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Vô hiệu hóa logging để tránh crash
    os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
    os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
    os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
    os.environ['TF_USE_LEGACY_CPU'] = '0'   # Thêm mới
    os.environ['TF_DISABLE_MKL'] = '1'      # Thêm mới
    os.environ['PYTHONWARNINGS'] = 'ignore::DeprecationWarning,ignore::UserWarning'

    try:
        service = Service(log_path=os.devnull)  # Tắt log của Selenium
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Thêm timeout
        driver.set_page_load_timeout(30)
        driver.set_script_timeout(30)
        return driver
    except Exception as e:
        raise RuntimeError(f"Lỗi khi khởi tạo Chrome driver: {e}")

class ManhuavnCrawler(BaseCrawler):
    """Crawler cho trang Manhuavn sử dụng multiprocessing"""
    
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
            driver = setup_driver()
            try:
                raw_comics = self.get_all_stories(driver, self.max_pages, progress_callback)
                logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
            finally:
                driver.quit()
                
            # Xử lý theo batch để kiểm soát tài nguyên tốt hơn
            batch_size = min(100, len(raw_comics))
            
            for i in range(0, len(raw_comics), batch_size):
                batch = raw_comics[i:i+batch_size]
                logger.info(f"Xử lý batch {i//batch_size + 1}/{(len(raw_comics)-1)//batch_size + 1} ({len(batch)} truyện)")
                
                # Chuẩn bị tham số cho worker - LOẠI BỎ các biến đồng bộ hóa
                worker_params = [(comic, self.db_manager.db_folder, self.base_url) for comic in batch]
                
                # Tạo và quản lý pool processes
                try:
                    # Sử dụng timeout cho toàn bộ pool thay vì từng task
                    with multiprocessing.Pool(processes=self.worker_count, initializer=init_process) as pool:
                        try:
                            # Sử dụng map thay vì map_async để đơn giản hóa
                            results = pool.map(process_comic_worker, worker_params, chunksize=1)
                            
                            # Lọc ra các kết quả không None
                            valid_results = [r for r in results if r is not None]
                            
                            # Cập nhật số lượng truyện đã xử lý
                            batch_comics_count = len(valid_results)
                            comics_count += batch_comics_count
                            
                            logger.info(f"Kết thúc batch {i//batch_size + 1}: Đã xử lý {batch_comics_count}/{len(batch)} truyện trong batch")
                            
                            # Cập nhật tiến độ
                            if progress_callback and len(raw_comics) > 0:
                                progress = (i + batch_comics_count) / len(raw_comics) * 100
                                progress_callback.emit(int(progress))
                            
                        except Exception as e:
                            logger.error(f"Lỗi khi xử lý map trong pool: {e}")
                            
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý batch: {e}")
                
                # Gọi garbage collector
                import gc
                gc.collect()
                
                # Pause nhỏ giữa các batch để giải phóng tài nguyên
                logger.info("Nghỉ giữa các batch để giải phóng tài nguyên...")
                time.sleep(3)
                
        except Exception as e:
            logger.error(f"Lỗi trong quá trình crawl: {e}")
        finally:
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
                        title = get_text_safe(item, ".story_title")
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
    
    def crawl_comments(self, comic, time_limit=None, days_limit=None):
        driver = self.setup_driver()
        comments = []
        old_comments_count = 0
        
        try:
            # Lấy link từ comic
            link = comic.get("link_truyen")
            comic_id = comic.get("id")
            
            if not link or not comic_id:
                logger.error(f"Không tìm thấy link hoặc ID truyện: {comic.get('ten_truyen', 'Unknown')}")
                driver.quit()
                return []
            
            if time_limit:
                logger.info(f"Crawl comment cho truyện: {comic.get('ten_truyen')} từ {days_limit} ngày gần đây ({time_limit.strftime('%Y-%m-%d')})")
            else:
                logger.info(f"Crawl tất cả comment cho truyện: {comic.get('ten_truyen')}")
            
            try:
                driver.get(link)
            except Exception as e:
                logger.error(f"Lỗi khi truy cập URL {link}: {str(e)}")
                driver.quit()
                return []
            
            time.sleep(random.uniform(2, 3))
            load_more_attempts = 0
            stop_loading = False
            max_load_attempts = 20  
            
            while load_more_attempts < max_load_attempts and not stop_loading:
                try:
                    if time_limit:
                        current_comments = driver.find_elements(By.CSS_SELECTOR, ".comment_item")
                        
                        # Kiểm tra comment cuối cùng 
                        if current_comments:
                            last_comment = current_comments[-1]
                            try:
                                time_span = last_comment.find_element(By.CSS_SELECTOR, "div.comment-head > span.time")
                                time_text = time_span.get_attribute("datetime") or time_span.text.strip()
                                
                                if time_text:
                                    comment_time = self.parse_relative_time(time_text)
                                    
                                    # Kiểm tra nếu comment cuối cùng đã quá cũ
                                    if comment_time < time_limit:
                                        logger.info(f"Dừng tải thêm comment: Đã phát hiện comment quá cũ ({time_text})")
                                        stop_loading = True
                                        break
                            except Exception as te:
                                logger.debug(f"Không thể lấy thời gian của comment cuối: {str(te)}")
                    
                    # Tìm và nhấp nút tải thêm comment
                    load_more_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button")
                    load_more_button.click()
                    load_more_attempts += 1
                    time.sleep(2)
                except Exception as e:
                    # Không tìm thấy nút hoặc lỗi khác - có thể đã tải hết comment
                    logger.debug(f"Không thể tải thêm comment: {str(e)}")
                    break

            # Lấy danh sách tất cả các comment đã tải
            comment_elements = driver.find_elements(By.CSS_SELECTOR, ".comment_item")
            logger.info(f"Đã tìm thấy {len(comment_elements)} comment")
            
            # Xử lý từng comment
            for comment_elem in comment_elements:
                try:
                    user = self.get_text_safe(comment_elem, ".comment-head")
                    content = self.get_text_safe(comment_elem, ".comment-content")
                    
                    time_text = ""
                    try:
                        time_span = comment_elem.find_element(By.CSS_SELECTOR, "div.comment-head > span.time")
                        time_text = time_span.get_attribute("datetime") or time_span.text.strip()
                    except:
                        pass
                    
                    comment_time = datetime.now()
                    if time_text:
                        comment_time = self.parse_relative_time(time_text)
                    
                    if time_limit and comment_time < time_limit:
                        logger.debug(f"Bỏ qua comment quá cũ: {time_text} ({comment_time.strftime('%Y-%m-%d')} < {time_limit.strftime('%Y-%m-%d')})")
                        old_comments_count += 1
                        continue
                    
                    if not content or content.strip() == "":
                        content = "N/A"
                        
                    if not user or user.strip() == "":
                        user = "Người dùng ẩn danh"
                    
                    comments.append({
                        "ten_nguoi_binh_luan": user,
                        "noi_dung": content,
                        "comic_id": comic_id,
                        "thoi_gian_binh_luan": comment_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "thoi_gian_cap_nhat": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý comment: {e}")
            
            # Lưu comments vào database
            if comments:
                logger.info(f"Lưu {len(comments)} comment cho truyện ID {comic_id}")
                self.sqlite_helper.save_comments_to_db(comic_id, comments, "Manhuavn")
            
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
    
    def parse_relative_time(self, time_text):
        if not time_text or not time_text.strip():
            return datetime.now()
            
        time_text = time_text.strip().lower()
        logger.debug(f"Đang phân tích chuỗi thời gian: '{time_text}'")
        
        try:
            if " - " in time_text and "/" in time_text:
                time_parts = time_text.split(" - ")
                if len(time_parts) == 2:
                    time_part = time_parts[0].strip()  
                    date_part = time_parts[1].strip()  
                    
                    # Tách ngày thành các thành phần
                    date_components = date_part.split("/")
                    if len(date_components) == 3:
                        day = date_components[0]
                        month = date_components[1]
                        year = date_components[2]
                        
                        # Chuyển đổi sang định dạng ISO
                        formatted_time = f"{year}-{month}-{day} {time_part}:00"
                        return datetime.strptime(formatted_time, "%Y-%m-%d %H:%M:%S")
            
            now = datetime.now()
            
            if "vừa xong" in time_text or "giây trước" in time_text:
                return now
            
            digits = ''.join(filter(str.isdigit, time_text))
            if not digits:
                return now  
            
            number = int(digits)
            
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