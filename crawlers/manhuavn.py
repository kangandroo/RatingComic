from crawlers.base_crawler import BaseCrawler
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import re
from datetime import datetime

class ManhuaVNCrawler(BaseCrawler):
    """Crawler cho website ManhuaVN"""
    
    def __init__(self, logger=None, chromedriver_path=None):
        super().__init__(logger, chromedriver_path = r"C:\Users\Hi\rating_comic\System\crawlers\chromedriver.exe")
        self.name = "ManhuaVN"
        self.base_url = "https://manhuavn.top"
    
    def get_all_stories(self, num_pages=3, emit_log=None):
        """Lấy danh sách truyện từ nhiều trang của ManhuaVN"""
        driver = self.setup_driver()
        stories = []
        
        try:
            for page in range(1, num_pages + 1):
                self.log(f"Đang crawl trang {page}/{num_pages} từ {self.name}", emit_log=emit_log)
                
                url = f"{self.base_url}/danhsach/P{page}/index.html?status=0&sort=2"
                driver.get(url)
                time.sleep(random.uniform(2, 4))
                
                # Chờ cho các phần tử truyện hiển thị
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".lst_story .story_item"))
                )
                
                # Lấy tất cả các phần tử truyện
                story_elements = driver.find_elements(By.CSS_SELECTOR, ".lst_story .story_item")
                
                for story in story_elements:
                    try:
                        # Lấy thông tin cơ bản
                        title = self.get_text_safe(story, ".story_title")
                        url = story.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                        
                        # Lấy ảnh bìa
                        try:
                            cover_url = story.find_element(By.CSS_SELECTOR, "img").get_attribute("src")
                        except:
                            cover_url = ""
                        
                        # Thêm vào danh sách
                        stories.append({
                            'title': title,
                            'url': url,
                            'cover_url': cover_url,
                            'chapter_count': 0,  # Sẽ cập nhật trong get_story_details
                            'views': 0,  # Sẽ cập nhật trong get_story_details
                            'source': 'manhuavn'
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
            story["status"] = self.get_text_safe(driver, ".info-row .contiep")
            story["follows"] = self.extract_number(self.get_text_safe(driver, "li.info-row strong"))
            story["views"] = self.extract_number(self.get_text_safe(driver, "li.info-row view.colorblue"))
            story["rating"] = self.get_text_safe(driver, 'span[itemprop="ratingValue"]')
            story["rating_count"] = self.extract_number(self.get_text_safe(driver, 'span[itemprop="ratingCount"]'))
            story["description"] = self.get_text_safe(driver, "li.clearfix p")
            
            # Lấy số chương
            chapter_text = self.get_text_safe(driver, "li.info-row a.colorblue")
            if chapter_text != "N/A":
                match = re.search(r'\d+', chapter_text)
                if match:
                    story["chapter_count"] = int(match.group())
            
            # Lấy tác giả
            try:
                author_element = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/ul/li[6]/a")
                story["author"] = author_element.text.strip()
            except:
                story["author"] = "N/A"
                
            # Lấy thể loại
            try:
                genre_elements = driver.find_elements(By.CSS_SELECTOR, ".info-row a[href*='theloai']")
                story["genres"] = [genre.text.strip() for genre in genre_elements]
            except:
                story["genres"] = []
                
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
            
            # Mở tất cả các bình luận
            max_attempts = 5  # Giới hạn số lần thử
            for _ in range(max_attempts):
                try:
                    load_more_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button")
                    driver.execute_script("arguments[0].click();", load_more_button)
                    time.sleep(2)
                except:
                    break
            
            # Lấy các bình luận
            comment_elements = driver.find_elements(By.CSS_SELECTOR, "li.comment_item")
            
            for comment_elem in comment_elements:
                username = self.get_text_safe(comment_elem, ".comment-head")
                content = self.get_text_safe(comment_elem, ".comment-content")
                
                if content and content != "N/A":
                    comments.append({
                        "username": username if username and username != "N/A" else "Ẩn danh",
                        "content": content,
                        "date": datetime.now()  # Không có thông tin ngày
                    })
                    
            self.log(f"Đã lấy {len(comments)} bình luận cho truyện {story.get('title', '')}", emit_log=emit_log)
                
        except Exception as e:
            self.log(f"Lỗi khi lấy bình luận: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return comments