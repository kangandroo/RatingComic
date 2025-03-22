import time
import random
import logging
import os
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures
import queue
import threading
from utils.sqlite_helper import SQLiteHelper


from crawlers.base_crawler import BaseCrawler

logger = logging.getLogger(__name__)

class NetTruyenCrawler(BaseCrawler):
    """Crawler cho website NetTruyen"""
    
    def __init__(self, db_manager, config_manager, base_url="https://nettruyenvie.com", max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        self.base_url = base_url
        self.max_pages = max_pages
        self.worker_count = worker_count
        self.db_manager.set_source("NetTruyen")  # Đặt nguồn dữ liệu mặc định
    
    def setup_driver(self):
        """Khởi tạo trình duyệt Chrome"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--lang=vi")
        
        try:
            # Lấy đường dẫn đến ChromeDriver từ config
            chromedriver_path = r"C:\Users\Hi\rating_comic\code\RatingComic\Test\crawlers\chromedriver.exe"
            
            # Kiểm tra xem chromedriver_path có tồn tại không
            if chromedriver_path and os.path.exists(chromedriver_path):
                logger.info(f"Sử dụng ChromeDriver từ: {chromedriver_path}")
                return webdriver.Chrome(service=Service(chromedriver_path), options=chrome_options)
            else:
                # Nếu không có đường dẫn hoặc không tồn tại, để Selenium tự tìm chromedriver
                # logger.warning("Không tìm thấy ChromeDriver, sử dụng mặc định của hệ thống")
                return webdriver.Chrome(options=chrome_options)
                
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Chrome driver: {e}")
            # Fallback: thử không sử dụng Service
            return webdriver.Chrome(options=chrome_options)
    
    def get_text_safe(self, element, selector):
        """Lấy text an toàn từ phần tử"""
        try:
            return element.find_element(By.CSS_SELECTOR, selector).text.strip()
        except Exception:
            return "N/A"
    
    def extract_chapter_number(self, chapter_text):
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
    
    def extract_number(self, text_value):
        """Trích xuất số từ các chuỗi với nhiều định dạng"""
        if not text_value or text_value == "N/A":
            return 0
            
        # Loại bỏ dấu chấm phân cách hàng nghìn và các ký tự không phải số
        try:
            return int(text_value.replace(".", "").replace(",", ""))
        except Exception:
            return 0
    
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản từ NetTruyen với SQLiteHelper"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Đặt nguồn dữ liệu
            self.db_manager.set_source("NetTruyen")
            
            # Khởi tạo SQLiteHelper
            sqlite_helper = SQLiteHelper(self.db_manager.db_folder)
            
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
                        sqlite_helper.save_comic_to_db(db_comic, "NetTruyen")
                        processed_count += 1
                        
                        # Cập nhật tiến độ
                        if progress_callback and len(raw_comics) > 0:
                            with lock:
                                progress = (processed_count / len(raw_comics)) * 100
                                progress_callback.emit(int(progress))
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
            
            # Dùng lock để đồng bộ cập nhật progress_callback
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
            elapsed_time = time.time() - start_time
            logger.info(f"Đã crawl {comics_count} truyện trong {elapsed_time:.2f} giây")
        
        return {
            "count": comics_count,
            "time_taken": time.time() - start_time,
            "website": "NetTruyen"
        }
    
    def get_all_stories(self, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        driver = self.setup_driver()
        stories = []
        page = 1
        
        try:
            while max_pages is None or page <= max_pages:
                url = f"{self.base_url}/?page={page}"
                logger.info(f"Đang tải trang {page}: {url}")
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".items .row .item"))
                    )
                except Exception:
                    logger.info(f"Không tìm thấy phần tử truyện trên trang {page}, kết thúc")
                    break

                # Lấy tất cả các item truyện
                item_elements = driver.find_elements(By.CSS_SELECTOR, ".items .row .item")
                
                if not item_elements:
                    logger.info("Không tìm thấy truyện nào trên trang, kết thúc")
                    break
                    
                for item in item_elements:
                    try:
                        # Lấy tiêu đề và link truyện
                        title_element = item.find_element(By.CSS_SELECTOR, "figcaption h3 a")
                        title = title_element.text.strip() if title_element.text else "Không có tên"
                        link = title_element.get_attribute("href")
                        
                        # Lấy thông tin chương
                        chapter_info = "Chapter 0"  # Giá trị mặc định
                        try:
                            chapter_info_elements = item.find_elements(By.CSS_SELECTOR, "figcaption ul li a")
                            if chapter_info_elements:
                                chapter_info = chapter_info_elements[0].get_attribute("title") or "Chapter 0"
                        except Exception:
                            pass
                        
                        # Trích xuất số chương
                        chapter_count = self.extract_chapter_number(chapter_info)
                        
                        if link:
                            stories.append({
                                "Tên truyện": title, 
                                "Link truyện": link,
                                "Số chương": chapter_count
                            })
                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý truyện: {e}")
                        continue

                # # Kiểm tra có trang tiếp theo không
                # next_buttons = driver.find_elements(By.CSS_SELECTOR, ".pagination a.next")
                # if not next_buttons:
                #     logger.info("Không tìm thấy nút next, kết thúc crawl")
                #     break

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

            # Lấy thông tin cơ bản
            story["Tác giả"] = self.get_text_safe(driver, "li.author.row p.col-xs-8")
            story["Trạng thái"] = self.get_text_safe(driver, "li.status.row p.col-xs-8")
            story["Đánh giá"] = self.get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(1)")
            
            # Lấy số liệu - xử lý an toàn với số liệu
            follow_text = self.get_text_safe(driver, ".follow span b.number_follow")
            story["Lượt theo dõi"] = self.extract_number(follow_text)
            
            view_text = self.get_text_safe(driver, "ul.list-info li:last-child p.col-xs-8")
            story["Lượt xem"] = self.extract_number(view_text)
            
            rating_count_text = self.get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(3)")
            story["Lượt đánh giá"] = self.extract_number(rating_count_text)

            # Cố gắng lấy số chương chính xác
            try:
                # Thử tìm chương mới nhất
                chapter_element = driver.find_element(By.CSS_SELECTOR, ".list-chapter li:first-child a")
                if chapter_element:
                    chapter_text = chapter_element.get_attribute("title")
                    if chapter_text:
                        chapter_count = self.extract_chapter_number(chapter_text)
                        if chapter_count > 0:
                            story["Số chương"] = chapter_count
                    
                # Nếu không tìm thấy số chương hoặc số chương = 0, thử đếm các chương
                if story.get("Số chương", 0) == 0:
                    chapter_items = driver.find_elements(By.CSS_SELECTOR, ".list-chapter li")
                    story["Số chương"] = len(chapter_items)
            except Exception as e:
                logger.error(f"Lỗi khi lấy số chương: {e}")

            # Lấy số bình luận
            try:
                comment_count_text = self.get_text_safe(driver, ".comment-count")
                story["Số bình luận"] = self.extract_number(comment_count_text)
            except:
                story["Số bình luận"] = 0

        except Exception as e:
            logger.error(f"Lỗi khi lấy thông tin chi tiết truyện {story.get('Tên truyện')}: {e}")
            return None
        finally:
            driver.quit()
            
        return story
    
    def transform_comic_data(self, raw_comic):
        """Chuyển đổi dữ liệu raw sang định dạng database"""
        return {
            "ten_truyen": raw_comic.get("Tên truyện", ""),
            "tac_gia": raw_comic.get("Tác giả", "N/A"),
            # "the_loai": raw_comic.get("Thể loại", ""),
            # "mo_ta": raw_comic.get("Mô tả", ""),
            "link_truyen": raw_comic.get("Link truyện", ""),
            "so_chuong": raw_comic.get("Số chương", 0),
            "luot_xem": raw_comic.get("Lượt xem", 0),
            "luot_theo_doi": raw_comic.get("Lượt theo dõi", 0),
            "rating": raw_comic.get("Đánh giá", ""),
            "luot_danh_gia": raw_comic.get("Lượt đánh giá", 0),
            "so_binh_luan": raw_comic.get("Số bình luận", 0),
            "trang_thai": raw_comic.get("Trạng thái", ""),
            "nguon": "NetTruyen",
            # Ước tính lượt thích (NetTruyen không có mục này)
            # "luot_thich": int(raw_comic.get("Lượt theo dõi", 0) * 0.7)
        }
    
    def crawl_comments(self, comic):
        """Crawl comment cho một truyện cụ thể - trong thread phân tích"""
        driver = self.setup_driver()
        comments = []
        unique_contents = set()
        duplicate_found = False
        
        try:
            # Lấy link từ comic
            link = comic.get("link_truyen")
            if not link:
                logger.error(f"Không tìm thấy link truyện cho: {comic.get('ten_truyen')}")
                return []
            
            logger.info(f"Đang crawl comment cho truyện: {comic.get('ten_truyen')}")
            
            driver.get(link)
            time.sleep(random.uniform(2, 3))

            # Gọi hàm joinComment() để chuyển đến phần comment
            try:
                driver.execute_script("joinComment()")
                time.sleep(random.uniform(2, 3))
            except Exception:
                logger.warning("Không thể gọi hàm joinComment")
                
            page_comment = 1
            max_comment_pages = 1000  # Giới hạn số trang bình luận
            
            while page_comment <= max_comment_pages:
                comments_in_current_page = 0

                # Lấy danh sách bình luận với selector cụ thể
                try:
                    comment_elements = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".info"))
                    )
                    
                    if not comment_elements:
                        logger.info("Không tìm thấy comment trong trang")
                        break
                        
                    for comment in comment_elements:
                        # Lấy tên người bình luận
                        try:
                            name_elem = comment.find_element(By.CSS_SELECTOR, ".comment-header span.authorname.name-1")
                            name = name_elem.text.strip() if name_elem.text else "Người dùng ẩn danh"
                        except:
                            name = "Người dùng ẩn danh"
                        
                        # Lấy nội dung bình luận
                        try:
                            content_elem = comment.find_element(By.CSS_SELECTOR, ".info div.comment-content")
                            content = content_elem.text.strip() if content_elem.text else "N/A"
                        except:
                            content = "N/A"
                        
                        # Kiểm tra xem nội dung bình luận đã tồn tại chưa
                        if content != "N/A" and content in unique_contents:
                            duplicate_found = True
                            logger.info("Phát hiện comment trùng lặp, sẽ dừng crawl")
                            break
                        
                        # Nếu nội dung khác N/A, thêm vào tập hợp để kiểm tra trùng lặp sau này
                        if content != "N/A":
                            unique_contents.add(content)
                        
                        # Thêm comment vào danh sách kết quả - chuyển đổi tên trường
                        comments.append({
                            "ten_nguoi_binh_luan": name,
                            "noi_dung": content,
                            "comic_id": comic.get("id")
                        })
                        comments_in_current_page += 1
                    
                    # Nếu phát hiện bình luận trùng lặp, dừng việc chuyển trang
                    if duplicate_found:
                        break
                    
                    # Thêm điều kiện dừng: Nếu số lượng bình luận trong trang < 10
                    if comments_in_current_page < 10:
                        logger.info(f"Số lượng comment trang {page_comment} < 10, dừng crawl")
                        break
                        
                except Exception as e:
                    logger.error(f"Lỗi khi lấy comments trang {page_comment}: {e}")
                    break

                # Nếu phát hiện bình luận trùng lặp, không tiếp tục chuyển trang
                if duplicate_found:
                    break

                # Tìm nút chuyển trang bình luận
                try:
                    # Thử nhiều cách để tìm nút "Sau" hoặc "Next"
                    next_button_selectors = [
                        "/html/body/form/main/div[3]/div/div[1]/div/div/div[2]/div[6]/ul/li[6]/a",
                        "//a[contains(text(), 'Sau')]",
                        "//a[contains(text(), 'Next')]",
                        "//li[contains(@class, 'next')]/a"
                    ]
                    
                    next_button = None
                    for selector in next_button_selectors:
                        next_buttons = driver.find_elements(By.XPATH, selector)
                        if next_buttons:
                            next_button = next_buttons[0]
                            break
                            
                    if next_button:
                        driver.execute_script("arguments[0].click();", next_button)
                        page_comment += 1
                        logger.info(f"Chuyển sang trang comment {page_comment}")
                        time.sleep(random.uniform(2, 3))
                    else:
                        logger.info("Không tìm thấy nút chuyển trang, kết thúc")
                        break
                except Exception as e:
                    logger.error(f"Lỗi khi chuyển trang comment: {e}")
                    break
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {e}")
        finally:
            driver.quit()
            
        logger.info(f"Đã crawl được {len(comments)} comment cho truyện {comic.get('ten_truyen')}")
        return comments