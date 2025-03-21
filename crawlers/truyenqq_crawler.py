from crawlers.base_crawler import BaseCrawler
import time
import random
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import concurrent.futures

logger = logging.getLogger(__name__)

class TruyenQQCrawler(BaseCrawler):
    """Crawler cho trang TruyenQQ dựa trên code mẫu đã được tối ưu"""
    
    def __init__(self, db_manager, base_url=None, max_pages=None, worker_count=5):
        super().__init__(db_manager, base_url, max_pages)
        
        if not base_url:
            self.base_url = "https://truyenqqto.com"
            
        # Số lượng worker cho xử lý đa luồng
        self.worker_count = worker_count
            
        # User agent để tránh bị chặn
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
        }
    
    def create_chrome_driver(self):
        """Tạo và cấu hình Chrome WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
        
        return webdriver.Chrome(options=chrome_options)
    
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
    
    def crawl_basic_data(self, progress_callback=None):
        """Crawl dữ liệu cơ bản của truyện từ trang TruyenQQ"""
        start_time = time.time()
        all_comics = []
        page_num = 1
        
        try:
            logger.info(f"Bắt đầu crawl từ {self.base_url}")
            
            # Số trang tối đa
            max_pages = self.max_pages if self.max_pages else 999
            
            # Duyệt qua từng trang
            while page_num <= max_pages:
                driver = self.create_chrome_driver()
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
                            progress = min(100, (page_num / max_pages) * 100)
                            progress_callback.emit(progress)
                        
                    except Exception as e:
                        logger.warning(f"Không thể lấy dữ liệu từ trang {page_num}: {str(e)}")
                        break  # Gặp lỗi, dừng vòng lặp
                        
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập trang {page_num}: {str(e)}")
                    break  # Gặp lỗi, dừng vòng lặp
                    
                finally:
                    driver.quit()
                    # Nghỉ ngẫu nhiên giữa các yêu cầu để tránh bị chặn
                    time.sleep(random.uniform(1, 3))
            
            # Xử lý chi tiết từng truyện
            logger.info(f"Tổng cộng {len(all_comics)} truyện cần lấy thông tin chi tiết")
            
            # Xử lý song song
            processed_comics = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.worker_count) as executor:
                # Chỉ lấy chi tiết cơ bản, không lấy comment
                futures = [executor.submit(self.crawl_comic_details, comic) for comic in all_comics]
                
                total = len(futures)
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        detailed_comic = future.result()
                        processed_comics.append(detailed_comic)
                        
                        # Cập nhật tiến trình
                        if progress_callback:
                            processed_pct = min(100, 50 + (i / total) * 50)  # 50% cho crawl cơ bản + 50% cho chi tiết
                            progress_callback.emit(processed_pct)
                        
                        # Log tiến trình
                        if (i + 1) % 5 == 0 or i + 1 == total:
                            logger.info(f"Đã xử lý chi tiết: {i + 1}/{total} truyện")
                        
                    except Exception as e:
                        logger.error(f"Lỗi khi lấy chi tiết truyện: {str(e)}")
            
            # Lưu dữ liệu vào database
            for comic in processed_comics:
                self.db_manager.save_comic(comic)
            
            # Tính thời gian thực hiện
            time_taken = time.time() - start_time
            
            # Cập nhật tiến trình cuối cùng
            if progress_callback:
                progress_callback.emit(100)
            
            logger.info(f"Đã hoàn thành crawl, thu thập được {len(processed_comics)} truyện")
            
            return {
                "count": len(processed_comics),
                "website": "TruyenQQ",
                "time_taken": time_taken
            }
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl dữ liệu: {str(e)}")
            raise
    
    def crawl_comic_details(self, comic):
        """Crawl thông tin chi tiết của một truyện"""
        driver = self.create_chrome_driver()
        
        try:
            comic_url = comic["link_truyen"]
            logger.debug(f"Đang crawl chi tiết truyện: {comic_url}")
            
            driver.get(comic_url)
            time.sleep(random.uniform(1, 2))
            
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
            
            # Lấy thể loại
            genre_elems = driver.find_elements(By.CSS_SELECTOR, "li.kind.row p.col-xs-9 a")
            the_loai = ", ".join([elem.text.strip() for elem in genre_elems]) if genre_elems else "N/A"
            comic["the_loai"] = the_loai
            
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
            
            # Đếm số bình luận
            comment_count = 0
            try:
                comment_elems = driver.find_elements(By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment")
                comment_count = len(comment_elems)
            except:
                pass
            
            comic["so_binh_luan"] = comment_count
            
            # Chuyển các giá trị sang số
            try:
                comic["luot_xem"] = self.extract_number(comic["luot_xem"])
                comic["luot_thich"] = self.extract_number(comic["luot_thich"])
                comic["luot_theo_doi"] = self.extract_number(comic["luot_theo_doi"])
            except:
                pass
            
            return comic
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl chi tiết truyện: {str(e)}")
            # Đảm bảo vẫn trả về đối tượng comic với thông tin cơ bản
            return comic
            
        finally:
            driver.quit()
    
    def crawl_comments(self, comic):
        """Crawl comment cho một truyện cụ thể"""
        driver = None
        all_comments = []
        
        try:
            comic_url = comic.get("link_truyen")
            logger.info(f"Đang crawl comment cho truyện: {comic['ten_truyen']}")
            
            driver = self.create_chrome_driver()
            driver.get(comic_url)
            time.sleep(random.uniform(1, 2))
            
            # Lặp qua các trang comment
            page_comment = 1
            while True:
                try:
                    # Gọi hàm loadComment để tải comment trang tiếp theo
                    driver.execute_script(
                        "if (typeof loadComment === 'function') { loadComment(arguments[0]); } else { throw 'loadComment not found'; }", 
                        page_comment
                    )
                except Exception:
                    break
                    
                time.sleep(random.uniform(1, 2))
                
                # Đợi để comment được tải
                try:
                    comments = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment"))
                    )
                except:
                    break
                    
                if not comments:
                    break
                    
                # Xử lý từng comment
                for comment in comments:
                    name = self.get_text_safe(comment, "div.outsite-comment div.outline-content-comment div:nth-child(1) strong")
                    content = self.get_text_safe(comment, "div.outsite-comment div.outline-content-comment div.content-comment")
                    
                    # Đảm bảo nội dung bình luận không để trống
                    if not content or content.strip() == "":
                        content = "N/A"
                        
                    # Đảm bảo tên người bình luận không để trống
                    if not name or name.strip() == "":
                        name = "N/A"
                    
                    comment_data = {
                        "ten_nguoi_binh_luan": name,
                        "noi_dung": content,
                        "story_id": comic.get("id")
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
                
                # Chuyển đến trang tiếp theo
                page_comment += 1
            
            logger.info(f"Đã crawl được {len(all_comments)} comment cho truyện: {comic['ten_truyen']}")
            
            return all_comments
            
        except Exception as e:
            logger.error(f"Lỗi khi crawl comment: {str(e)}")
            return []
            
        finally:
            if driver:
                driver.quit()
    
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