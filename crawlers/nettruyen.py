from crawlers.base_crawler import BaseCrawler
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import re
from datetime import datetime

class NetTruyenCrawler(BaseCrawler):
    """Crawler cho website NetTruyen"""
    
    def __init__(self, logger=None, chromedriver_path=None):
        super().__init__(logger, chromedriver_path = r"C:\Users\Hi\rating_comic\code\RatingComic\crawlers\chromedriver.exe")
        self.name = "NetTruyen"
        self.base_url = "https://nettruyenvie.com"
    
    def get_all_stories(self, num_pages=3, emit_log=None):
        """Lấy danh sách truyện từ nhiều trang của NetTruyen"""
        driver = self.setup_driver()
        stories = []
        
        try:
            for page in range(1, num_pages + 1):
                self.log(f"Đang crawl trang {page}/{num_pages} từ {self.name}", emit_log=emit_log)
                
                url = f"{self.base_url}/?page={page}"
                driver.get(url)
                time.sleep(random.uniform(2, 4))
                
                # Chờ cho các phần tử truyện hiển thị
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".items .row .item"))
                )
                
                # Lấy tất cả các phần tử truyện
                story_elements = driver.find_elements(By.CSS_SELECTOR, ".items .row .item")
                
                for story in story_elements:
                    try:
                        # Lấy thông tin cơ bản
                        title_element = story.find_element(By.CSS_SELECTOR, "figcaption h3 a")
                        title = title_element.text.strip() if title_element.text else "Không có tên"
                        url = title_element.get_attribute("href")
                        
                        # Lấy ảnh bìa
                        try:
                            cover_url = story.find_element(By.CSS_SELECTOR, "div.image img").get_attribute("src")
                        except:
                            cover_url = ""
                        
                        # Lấy thông tin chương
                        chapter_info = "Chapter 0"  # Giá trị mặc định
                        try:
                            chapter_info_elements = story.find_elements(By.CSS_SELECTOR, "figcaption ul li a")
                            if chapter_info_elements:
                                chapter_info = chapter_info_elements[0].get_attribute("title") or "Chapter 0"
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
                            'source': 'nettruyen'
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
            
            # Lấy thông tin chi tiết
            story["author"] = self.get_text_safe(driver, "li.author.row p.col-xs-8")
            story["status"] = self.get_text_safe(driver, "li.status.row p.col-xs-8")
            story["rating"] = self.get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(1)")
            
            # Lấy số lượt theo dõi, xem, đánh giá
            follows_text = self.get_text_safe(driver, ".follow span b.number_follow")
            views_text = self.get_text_safe(driver, "ul.list-info li:last-child p.col-xs-8")
            rating_count_text = self.get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(3)")
            
            story["follows"] = self.extract_number(follows_text)
            story["views"] = self.extract_number(views_text)
            story["rating_count"] = self.extract_number(rating_count_text)
            
                
            # Lấy mô tả
            story["description"] = self.get_text_safe(driver, ".detail-content p")
            
            # Lấy tên khác nếu có
            try:
                alt_title_element = driver.find_element(By.CSS_SELECTOR, "h2.other-name")
                if alt_title_element:
                    story["alt_title"] = alt_title_element.text.strip()
            except:
                story["alt_title"] = ""
            
            # Cập nhật số chương chính xác nếu có thể
            try:
                chapter_items = driver.find_elements(By.CSS_SELECTOR, ".list-chapter li")
                story["chapter_count"] = len(chapter_items)
            except:
                pass
            
        except Exception as e:
            self.log(f"Lỗi khi lấy chi tiết truyện: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return story
    
    def get_comments(self, story, emit_log=None):
        """Lấy bình luận của một truyện"""
        driver = self.setup_driver()
        comments = []
        unique_contents = set()
        
        try:
            self.log(f"Đang lấy bình luận cho truyện: {story.get('title', '')}", emit_log=emit_log)
                
            driver.get(story["url"])
            time.sleep(random.uniform(2, 3))
            
            # Gọi hàm joinComment() để chuyển đến phần comment
            try:
                driver.execute_script("joinComment()")
                time.sleep(random.uniform(2, 3))
            except:
                pass
                
            page_comment = 1
            max_comment_pages = 5  # Giới hạn số trang bình luận để tránh quá lâu
            
            while page_comment <= max_comment_pages:
                try:
                    # Lấy danh sách bình luận 
                    comment_elements = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".info"))
                    )
                    
                    if not comment_elements:
                        break
                        
                    for comment in comment_elements:
                        # Lấy tên người bình luận
                        name = self.get_text_safe(comment, ".comment-header span.authorname.name-1")
                        if not name or name == "N/A":
                            name = "Người dùng ẩn danh"
                        
                        # Lấy nội dung bình luận
                        content = self.get_text_safe(comment, ".info div.comment-content")
                        
                        # Kiểm tra trùng lặp
                        if content != "N/A" and content in unique_contents:
                            continue
                        
                        # Thêm vào tập hợp
                        if content != "N/A":
                            unique_contents.add(content)
                        
                        # Thêm vào danh sách bình luận
                        comments.append({
                            "username": name,
                            "content": content,
                            "date": datetime.now()
                        })
                        
                    # Thử tìm nút "Sau" hoặc "Next"
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
                        time.sleep(random.uniform(2, 3))
                    else:
                        break
                        
                except Exception as e:
                    self.log(f"Lỗi khi lấy trang bình luận {page_comment}: {str(e)}", level="error", emit_log=emit_log)
                    break
                    
            self.log(f"Đã lấy {len(comments)} bình luận cho truyện {story.get('title', '')}", emit_log=emit_log)
                
        except Exception as e:
            self.log(f"Lỗi khi lấy bình luận: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return comments