from crawlers.base_crawler import BaseCrawler
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from datetime import datetime

class TruyenQQCrawler(BaseCrawler):
    """Crawler cho website TruyenQQ"""
    
    def __init__(self, logger=None, chromedriver_path=None):
        super().__init__(logger, chromedriver_path = r"C:\Users\Hi\rating_comic\code\RatingComic\crawlers\chromedriver.exe")
        self.name = "TruyenQQ"
        self.base_url = "https://truyenqqto.com"
    
    def get_all_stories(self, num_pages=3, emit_log=None):
        """Lấy danh sách truyện từ nhiều trang của TruyenQQ"""
        driver = self.setup_driver()
        stories = []
        
        try:
            for page in range(1, num_pages + 1):
                self.log(f"Đang crawl trang {page}/{num_pages} từ {self.name}", emit_log=emit_log)
                
                url = f"{self.base_url}/truyen-moi-cap-nhat/trang-{page}.html"
                driver.get(url)
                time.sleep(random.uniform(2, 4))
                
                # Chờ cho các phần tử truyện hiển thị
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".list_grid li"))
                )
                
                # Lấy tất cả các phần tử truyện
                story_elements = driver.find_elements(By.CSS_SELECTOR, ".list_grid li")
                
                for story in story_elements:
                    try:
                        # Lấy thông tin cơ bản
                        title_element = story.find_element(By.CSS_SELECTOR, ".book_name.qtip h3 a")
                        title = title_element.get_attribute("title")
                        url = title_element.get_attribute("href")
                        
                        # Lấy ảnh bìa
                        try:
                            cover_url = story.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
                        except:
                            cover_url = ""
                        
                        # Lấy thông tin chương
                        chapter_info = "Chapter 0"  # Giá trị mặc định
                        try:
                            chapter_elements = story.find_elements(By.CSS_SELECTOR, ".last_chapter a")
                            if chapter_elements:
                                chapter_info = chapter_elements[0].get_attribute("title") or "Chapter 0"
                        except:
                            pass
                        
                        # Trích xuất số chương
                        chapter_count = self.extract_chapter_number(chapter_info)
                        
                        # Thêm vào danh sách
                        stories.append({
                            'title': title,
                            'url': url,
                            'cover_url': cover_url,
                            'chapter_count': chapter_count,
                            'views': 0,  # Sẽ cập nhật trong get_story_details
                            'likes': 0,  # Sẽ cập nhật trong get_story_details
                            'source': 'truyenqq'
                        })
                    except Exception as e:
                        self.log(f"Lỗi khi xử lý truyện: {str(e)}", level="error", emit_log=emit_log)
                
                self.log(f"Đã tìm thấy {len(stories)} truyện từ {page} trang", emit_log=emit_log)
                
        except Exception as e:
            self.log(f"Lỗi khi crawl: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return stories
    
    def get_story_details(self, story, emit_log=None):
        """Lấy thông tin chi tiết của một truyện"""
        driver = self.setup_driver()
        
        try:
            self.log(f"Đang lấy chi tiết truyện: {story.get('title', '')}", emit_log=emit_log)
                
            driver.get(story["url"])
            time.sleep(random.uniform(2, 3))
            
            # Kiểm tra xem có phần tử tên khác không để điều chỉnh selector
            has_other_name = len(driver.find_elements(By.CSS_SELECTOR, "li.othername.row h2")) > 0
            
            # Lấy tên khác nếu có
            if has_other_name:
                story["alt_title"] = self.get_text_safe(driver, "li.othername.row h2")
            else:
                story["alt_title"] = ""
            
            # Lấy tác giả
            story["author"] = self.get_text_safe(driver, "li.author.row p.col-xs-9 a")
            
            # Lấy trạng thái
            story["status"] = self.get_text_safe(driver, "li.status.row p.col-xs-9")
            
            # Lấy lượt thích, theo dõi và xem
            if has_other_name:
                likes_text = self.get_text_safe(driver, "li:nth-child(4) p.col-xs-9.number-like")
                follows_text = self.get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
                views_text = self.get_text_safe(driver, "li:nth-child(6) p.col-xs-9")
            else:
                likes_text = self.get_text_safe(driver, "li:nth-child(3) p.col-xs-9.number-like")
                follows_text = self.get_text_safe(driver, "li:nth-child(4) p.col-xs-9")
                views_text = self.get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
                
            story["likes"] = self.extract_number(likes_text)
            story["follows"] = self.extract_number(follows_text)
            story["views"] = self.extract_number(views_text)
            
            # Lấy thể loại
            # try:
            #     genre_elements = driver.find_elements(By.CSS_SELECTOR, ".list-info .kind p a")
            #     story["genres"] = [genre.text.strip() for genre in genre_elements]
            # except:
            #     story["genres"] = []
                
            # Lấy mô tả
            try:
                # Cố gắng nhấp vào "Xem thêm" nếu có
                try:
                    readmore_button = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "p > a"))
                    )
                    readmore_button.click()
                    time.sleep(1)
                except:
                    pass
                    
                story["description"] = self.get_text_safe(driver, "div.story-detail-info.detail-content")
            except:
                story["description"] = "Không có mô tả"
                
            # Cập nhật số chương
            try:
                chapter_elements = driver.find_elements(By.CSS_SELECTOR, ".works-chapter-list div.works-chapter-item")
                story["chapter_count"] = len(chapter_elements)
            except:
                pass  # Giữ nguyên số chương đã có
                
        except Exception as e:
            self.log(f"Lỗi khi lấy chi tiết truyện: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return story
    
    def get_comments(self, story, emit_log=None):
        """Lấy bình luận của một truyện"""
        driver = self.setup_driver()
        comments = []
        
        try:
            self.log(f"Đang lấy bình luận cho truyện: {story.get('title', '')}", emit_log=emit_log)
                
            driver.get(story["url"])
            time.sleep(random.uniform(2, 3))
            
            page_comment = 1
            max_comment_pages = 5  # Giới hạn số trang bình luận
            
            while page_comment <= max_comment_pages:
                try:
                    # Tải trang bình luận
                    driver.execute_script(
                        "if (typeof loadComment === 'function') { loadComment(arguments[0]); } else { throw 'loadComment not found'; }", 
                        page_comment
                    )
                    time.sleep(random.uniform(1, 2))
                    
                    # Kiểm tra có bình luận không
                    comment_elements = driver.find_elements(By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment")
                    
                    if not comment_elements:
                        break
                        
                    for comment_elem in comment_elements:
                        username = self.get_text_safe(comment_elem, "div.outsite-comment div.outline-content-comment div:nth-child(1) strong")
                        content = self.get_text_safe(comment_elem, "div.outsite-comment div.outline-content-comment div.content-comment")
                        
                        if not username:
                            username = "Ẩn danh"
                            
                        if content and content != "N/A":
                            comments.append({
                                "username": username,
                                "content": content,
                                "date": datetime.now()  # TruyenQQ không hiển thị ngày bình luận
                            })
                            
                    page_comment += 1
                    
                except Exception as e:
                    self.log(f"Lỗi khi tải trang bình luận {page_comment}: {str(e)}", level="error", emit_log=emit_log)
                    break
                    
            self.log(f"Đã lấy {len(comments)} bình luận cho truyện {story.get('title', '')}", emit_log=emit_log)
                
        except Exception as e:
            self.log(f"Lỗi khi lấy bình luận: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return comments