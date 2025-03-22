from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
import re
import os
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
        
        logger.info(f"Khởi tạo ManhuavnCrawler với base_url={self.base_url}")
        
    def setup_driver(self):
        """Khởi tạo trình duyệt Chrome"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            # Lấy đường dẫn đến ChromeDriver từ config
            chromedriver_path = self.config_manager.get_chrome_driver_path()
            
            # Kiểm tra xem chromedriver_path có tồn tại không
            if chromedriver_path and os.path.exists(chromedriver_path):
                logger.info(f"Sử dụng ChromeDriver từ: {chromedriver_path}")
                return webdriver.Chrome(service=Service(chromedriver_path), options=chrome_options)
            else:
                # Nếu không có đường dẫn hoặc không tồn tại, để Selenium tự tìm
                return webdriver.Chrome(options=chrome_options)
        except Exception as e:
            # logger.error(f"Lỗi khi khởi tạo Chrome driver: {e}")
            # Fallback: thử không sử dụng Service
            return webdriver.Chrome(options=chrome_options)
    
    def get_text_safe(self, element, selector):
        """Trích xuất nội dung văn bản an toàn từ phần tử"""
        try:
            return element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except Exception:
            return "N/A"
    
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản từ Manhuavn"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Lấy danh sách truyện
            raw_comics = self.get_all_stories(self.max_pages, progress_callback)
            logger.info(f"Đã lấy được {len(raw_comics)} truyện từ danh sách")
            
            # Tạo counter để theo dõi số lượng đã lưu
            processed_count = 0
            
            # Function để xử lý một truyện
            def process_comic(comic):
                nonlocal processed_count
                
                try:
                    # Lấy thông tin chi tiết
                    detailed_comic = self.get_story_details(comic)
                    
                    if detailed_comic:
                        # Chuyển đổi định dạng
                        db_comic = self.transform_comic_data(detailed_comic)
                        
                        # Lưu vào database bằng helper
                        self.sqlite_helper.save_comic_to_db(db_comic, "Manhuavn")
                        
                        # Cập nhật counter an toàn
                        with lock:
                            processed_count += 1
                            
                            # Cập nhật tiến độ
                            if progress_callback and len(raw_comics) > 0:
                                progress = (processed_count / len(raw_comics)) * 100
                                progress_callback.emit(int(progress))
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
            
            # Dùng lock để đồng bộ cập nhật biến đếm và progress_callback
            lock = threading.Lock()
            
            # Xử lý song song việc crawl + lưu dữ liệu
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_count) as executor:
                # Submit tất cả task
                futures = [executor.submit(process_comic, comic) for comic in raw_comics]
                
                # Đợi tất cả hoàn thành
                concurrent.futures.wait(futures)
            
            # Lấy số lượng đã xử lý
            comics_count = processed_count
                
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
    
    def get_all_stories(self, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        driver = self.setup_driver()
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
        finally:
            driver.quit()
            
        logger.info(f"Đã tìm thấy {len(stories)} truyện để crawl")
        return stories
    
    def get_story_details(self, story):
        """Lấy thông tin chi tiết của truyện"""
        driver = self.setup_driver()
        
        try:
            driver.get(story["Link truyện"])
            time.sleep(random.uniform(2, 3))

            # Lấy thông tin chi tiết
            story["Tình trạng"] = self.get_text_safe(driver, ".info-row .contiep")
            story["Lượt theo dõi"] = self.get_text_safe(driver, "li.info-row strong")
            story["Lượt xem"] = self.get_text_safe(driver, "li.info-row view.colorblue")
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
                
            # Lấy thể loại
            try:
                genre_elements = driver.find_elements(By.CSS_SELECTOR, "li.kind.row a")
                genres = [genre.text.strip() for genre in genre_elements if genre.text.strip()]
                story["Thể loại"] = ", ".join(genres)
            except Exception:
                story["Thể loại"] = "Chưa phân loại"

        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin chi tiết truyện {story.get('Tên truyện')}: {e}")
            return None
        finally:
            driver.quit()
            
        return story
    
    def transform_comic_data(self, raw_comic):
        """Chuyển đổi dữ liệu raw sang định dạng database"""
        # Chuyển đổi từ định dạng của Manhuavn sang định dạng chung
        return {
            "ten_truyen": raw_comic.get("Tên truyện", ""),
            "tac_gia": raw_comic.get("Tác giả", "N/A"),
            "the_loai": raw_comic.get("Thể loại", ""),
            "mo_ta": raw_comic.get("Mô tả", ""),
            "link_truyen": raw_comic.get("Link truyện", ""),
            "so_chuong": int(raw_comic.get("Số chương", "0")) if raw_comic.get("Số chương", "0").isdigit() else 0,
            "luot_xem": self.extract_number(raw_comic.get("Lượt xem", "0")),
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
            comic_id = comic.get("id")
            
            if not link or not comic_id:
                logger.error(f"Không tìm thấy link hoặc ID truyện: {comic.get('ten_truyen', 'Unknown')}")
                return []
            
            logger.info(f"Đang crawl comment cho truyện: {comic.get('ten_truyen')}")
            
            driver.get(link)
            time.sleep(random.uniform(2, 3))

            # Mở tất cả bình luận
            while True:
                try:
                    load_more_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button")
                    load_more_button.click()
                    time.sleep(2)
                except:
                    break

            # Lấy danh sách bình luận
            comment_elements = driver.find_elements(By.CSS_SELECTOR, "li.comment_item")
            
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