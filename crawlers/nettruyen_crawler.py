import time
import random
import logging
import os
import re
from datetime import datetime, timedelta
from seleniumbase import Driver
import concurrent.futures
import queue
import threading
from utils.sqlite_helper import SQLiteHelper
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from crawlers.base_crawler import BaseCrawler

logger = logging.getLogger(__name__)

class DriverPool:
    """Quản lý pool của WebDriver để tái sử dụng giữa các threads"""
    
    def __init__(self, max_drivers=10, setup_func=None):
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

class NetTruyenCrawler(BaseCrawler):
    """Crawler cho website NetTruyen sử dụng SeleniumBase để bypass Cloudflare"""
    
    def __init__(self, db_manager, config_manager, base_url="https://nettruyenvio.com", max_pages=None, worker_count=5):
        super().__init__(db_manager, config_manager)
        self.base_url = base_url
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

    def setup_driver(self):
        """Tạo và cấu hình SeleniumBase Driver để bypass Cloudflare"""
        try:
            # Thiết lập môi trường
            os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
            os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'
            os.environ['TF_FORCE_GPU_ALLOW_GROWTH'] = 'false'
            
            # Trong SeleniumBase, tham số có tên khác
            # Sử dụng minimal set của các tham số được hỗ trợ
            from seleniumbase import Driver
            
            driver = Driver(
                browser="chrome",   # Chỉ định trình duyệt
                uc=True,            # Sử dụng undetected chromedriver
                headless=True      # Chạy ở chế độ headless
            )
            
            # Thiết lập các timeout sau khi tạo driver
            driver.implicitly_wait(5)
            driver.set_page_load_timeout(30)
            
            return driver
            
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo SeleniumBase driver: {e}")
            try:
                # Fallback với ít tùy chọn hơn
                return Driver(browser="chrome", headless=True)
            except Exception as e2:
                logger.critical(f"Lỗi nghiêm trọng khi khởi tạo SeleniumBase driver: {e2}")
                raise RuntimeError(f"Không thể khởi tạo SeleniumBase driver: {e2}")
    
    def get_text_safe(self, driver, selector):
        """Lấy text an toàn từ phần tử"""
        try:
            return driver.find_element("css selector", selector).text.strip()
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
        """Crawl dữ liệu cơ bản từ NetTruyen với driver pool và xử lý theo batch"""
        start_time = time.time()
        comics_count = 0
        
        try:
            # Đặt nguồn dữ liệu
            self.db_manager.set_source("NetTruyen")
            
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
                        self.sqlite_helper.save_comic_to_db(db_comic, "NetTruyen")
                        
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
                                    logger.info(f"Hoàn thành {comics_count}/{len(raw_comics)}: {comic.get('Tên truyện', '')}")
                            except Exception as e:
                                logger.error(f"Lỗi khi xử lý truyện {comic.get('Tên truyện', '')}: {e}")
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
            
            # Đóng tất cả kết nối SQLite
            self.sqlite_helper.close_all_connections()
            
            elapsed_time = time.time() - start_time
            logger.info(f"Đã crawl {comics_count} truyện trong {elapsed_time:.2f} giây")
        
        return {
            "count": comics_count,
            "time_taken": time.time() - start_time,
            "website": "NetTruyen"
        }
    
    def get_all_stories(self, driver, max_pages=None, progress_callback=None):
        """Lấy danh sách truyện từ nhiều trang"""
        stories = []
        page = 1
        
        try:
            # Bypass Cloudflare trước tiên
            self.bypass_cloudflare(driver)
            
            while max_pages is None or page <= max_pages:
                url = f"{self.base_url}/?page={page}"
                logger.info(f"Đang tải trang {page}: {url}")
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                try:
                    # Wait for the stories to be loaded
                    driver.wait_for_element_present(".items .row .item", timeout=10)
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
    
    def bypass_cloudflare(self, driver):
        """Bypass Cloudflare protection"""
        try:
            url = f"{self.base_url}/?page={1}"
            logger.info(f"Đang truy cập {url} để bypass Cloudflare...")
            driver.get(url)
            
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
    
    def get_story_details(self, story, driver):
        """Lấy thông tin chi tiết của truyện sử dụng driver được cung cấp từ pool"""
        try:
            for attempt in range(5):
                try:
                    driver.get(story["Link truyện"])
                    driver.wait_for_element_present("li.author.row p.col-xs-8", timeout=10)
                    break
                except Exception as e:
                    logger.warning(f"Thử lần {attempt + 1}")
                    time.sleep(random.uniform(2, 4))
            else:
                logger.error("Không thể truy cập trang sau 5 lần thử")
                return None

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
                chapter_element = driver.find_element("css selector", ".list-chapter li:first-child a")
                if chapter_element:
                    chapter_text = chapter_element.get_attribute("title")
                    if chapter_text:
                        chapter_count = self.extract_chapter_number(chapter_text)
                        if chapter_count > 0:
                            story["Số chương"] = chapter_count
                    
                # Nếu không tìm thấy số chương hoặc số chương = 0, thử đếm các chương
                if story.get("Số chương", 0) == 0:
                    chapter_items = driver.find_elements("css selector", ".list-chapter li")
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
            
        return story
    
    def transform_comic_data(self, raw_comic):
        """Chuyển đổi dữ liệu raw sang định dạng database"""
        return {
            "ten_truyen": raw_comic.get("Tên truyện", ""),
            "tac_gia": raw_comic.get("Tác giả", "N/A"),
            "link_truyen": raw_comic.get("Link truyện", ""),
            "so_chuong": raw_comic.get("Số chương", 0),
            "luot_xem": raw_comic.get("Lượt xem", 0),
            "luot_theo_doi": raw_comic.get("Lượt theo dõi", 0),
            "rating": raw_comic.get("Đánh giá", ""),
            "luot_danh_gia": raw_comic.get("Lượt đánh giá", 0),
            "so_binh_luan": raw_comic.get("Số bình luận", 0),
            "trang_thai": raw_comic.get("Trạng thái", ""),
            "nguon": "NetTruyen",
        }
    
    def crawl_comments(self, comic, time_limit=None, days_limit=None):
        """Crawl comment cho một truyện cụ thể với giới hạn thời gian"""
        driver = self.setup_driver()
        comments = []
        unique_contents = set()
        old_comments_count = 0
        
        try:
            # Bypass Cloudflare trước tiên
            self.bypass_cloudflare(driver)
            
            # Lấy link từ comic
            link = comic.get("link_truyen")
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
                logger.warning("Không thể gọi hàm joinComment")
                
            page_comment = 1
            max_comment_pages = 100
            stop_crawling = False
            
            while page_comment <= max_comment_pages and not stop_crawling:
                comments_in_current_page = 0
                old_comments_in_page = 0
                
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".comment-list .item.clearfix .info"))
                    )
                    comment_elements = driver.find_elements("css selector", ".comment-list .item.clearfix .info")
                    
                    if not comment_elements:
                        break
                        
                    total_comments_in_page = len(comment_elements)
                    logger.info(f"Tìm thấy {total_comments_in_page} comment trên trang {page_comment}")
                    
                    # Xử lý từng comment
                    for comment in comment_elements:
                        try:
                            # Lấy tên người bình luận
                            try:
                                name_elem = comment.find_element("css selector", ".comment-header span.authorname.name-1")
                                name = name_elem.text.strip() if name_elem.text else "Người dùng ẩn danh"
                            except:
                                name = "Người dùng ẩn danh"
                            
                            # Lấy nội dung bình luận
                            try:
                                content_elem = comment.find_element("css selector", ".info .comment-content")
                                content = content_elem.text.strip() if content_elem.text else "N/A"
                            except:
                                content = "N/A"
                            
                            time_text = ""
                            try:
                                abbr_elem = comment.find_element("css selector", "ul.comment-footer .li .abbr")
                                
                                time_text = abbr_elem.get_attribute("title") or abbr_elem.text.strip()
                                logger.info(f"Thời gian comment raw: '{time_text}'")
                            except:
                                logger.debug("Không tìm thấy thẻ abbr chứa thời gian")
                            
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
                            
                            # Kiểm tra trùng lặp
                            if content != "N/A" and content in unique_contents:
                                logger.info("Phát hiện comment trùng lặp, dừng crawl")
                                stop_crawling = True
                                break
                            
                            if content != "N/A":
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
                            
                        if stop_crawling:
                            break
                    
                    # Thống kê kết quả trang hiện tại
                    logger.info(f"Trang {page_comment}: {comments_in_current_page} comment mới, {old_comments_in_page} comment quá cũ")
                    
                    # Chuyển trang
                    if stop_crawling:
                        logger.info("Dừng crawl do comment quá cũ")
                        break
                        
                    try:
                        next_button = None
                        selector = ["/html/body/form/main/div[3]/div/div[1]/div/div/div[2]/div[6]/ul/li[8]/a"]
                        try:
                            # Đợi tối đa 2 giây cho mỗi selector
                            WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, selector)))
                            buttons = driver.find_elements("xpath", selector)
                            if buttons:
                                next_button = buttons[0]
                                logger.info(f"Đã tìm thấy nút chuyển trang với selector: {selector}")
                                break
                        except Exception:
                            continue
                                
                        if next_button:
                            # Kiểm tra xem nút có thể nhấp được không
                            is_clickable = WebDriverWait(driver, 3).until(
                                EC.element_to_be_clickable((By.XPATH, "xpath_của_element"))
                            )
                            
                            # Scroll đến nút trước khi nhấp
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                            time.sleep(0.5)  # Đợi sau khi scroll
                            
                            # Click bằng JavaScript và kiểm tra cả click thông thường
                            try:
                                driver.execute_script("arguments[0].click();", next_button)
                            except Exception:
                                try:
                                    next_button.click()  # Thử click thông thường
                                except Exception as e:
                                    logger.warning(f"Không thể nhấp vào nút bằng cả hai phương pháp: {e}")
                                    
                            page_comment += 1
                            logger.info(f"Chuyển sang trang comment {page_comment}")
                            time.sleep(random.uniform(2, 3))
                        else:
                            logger.info("Không tìm thấy nút chuyển trang, kết thúc")
                            break
                    except Exception as e:
                        logger.info(f"Lỗi khi chuyển trang: {e}, kết thúc")
                        break
                    
                except Exception as e:
                    logger.error(f"Lỗi khi lấy comments trang {page_comment}: {e}")
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
    
    def parse_relative_time(self, time_text):
        """Chuyển đổi thời gian tương đối hoặc chính xác thành datetime"""
        now = datetime.now()
        time_text = time_text.lower().strip()
        
        try:
            try:
                return datetime.strptime(time_text, "%Y-%m-%d %H:%M:%S") 
            except ValueError:
                pass
            
            # Xử lý thời gian tương đối
            digits = ''.join(filter(str.isdigit, time_text))
            if not digits:  # "vừa xong", "vài giây trước"
                return now
                
            number = int(digits)
            
            if "phút" in time_text:
                return now - timedelta(minutes=number)
            elif "giờ" in time_text:
                return now - timedelta(hours=number)
            elif "ngày" in time_text:
                return now - timedelta(days=number)
            elif "tuần" in time_text:
                return now - timedelta(weeks=number)
            elif "tháng" in time_text:
                return now - timedelta(days=int(number * 30.44))
            elif "năm" in time_text:
                return now - timedelta(days=number * 365)
            
            return now
            
        except Exception as e:
            logger.error(f"Lỗi phân tích thời gian '{time_text}': {e}")
            return now