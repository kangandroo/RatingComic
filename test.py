from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
import logging
from datetime import datetime

class BaseCrawler:
    """Lớp cơ sở cho các trình thu thập dữ liệu."""
    
    def __init__(self, logger=None, chromedriver_path=None):
        self.logger = logger or logging.getLogger(__name__)
        self.chromedriver_path = chromedriver_path

    def setup_driver(self):
        """Khởi tạo ChromeDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Chạy chế độ không giao diện
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        service = Service(self.chromedriver_path)
        driver = webdriver.Chrome(service=service, options=chrome_options)
        return driver
    
    def log(self, message, level="info", emit_log=None):
        """Ghi log"""
        if level == "error":
            self.logger.error(message)
        else:
            self.logger.info(message)
        if emit_log:
            emit_log(message)

class TruyenQQCrawler(BaseCrawler):
    """Crawler cho website TruyenQQ"""
    
    def __init__(self, logger=None, chromedriver_path=None):
        super().__init__(logger, chromedriver_path)
        self.name = "TruyenQQ"
        self.base_url = "https://truyenqqto.com"
    
    def get_all_stories(self, num_pages=1, emit_log=None):
        """Lấy danh sách truyện từ nhiều trang"""
        driver = self.setup_driver()
        stories = []
        
        try:
            for page in range(1, num_pages + 1):
                self.log(f"Đang crawl trang {page}/{num_pages}", emit_log=emit_log)
                
                url = f"{self.base_url}/truyen-moi-cap-nhat/trang-{page}.html"
                driver.get(url)
                time.sleep(random.uniform(2, 4))

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".list_grid li"))
                )

                story_elements = driver.find_elements(By.CSS_SELECTOR, ".list_grid li")

                for story in story_elements:
                    try:
                        title_element = story.find_element(By.CSS_SELECTOR, ".book_name.qtip h3 a")
                        title = title_element.get_attribute("title")
                        url = title_element.get_attribute("href")

                        cover_url = story.find_element(By.CSS_SELECTOR, "img").get_attribute("src")

                        chapter_info = "Chapter 0"
                        chapter_elements = story.find_elements(By.CSS_SELECTOR, ".last_chapter a")
                        if chapter_elements:
                            chapter_info = chapter_elements[0].get_attribute("title") or "Chapter 0"

                        stories.append({
                            'title': title,
                            'url': url,
                            'cover_url': cover_url,
                            'chapter_info': chapter_info,
                            'source': 'truyenqq'
                        })
                    except Exception as e:
                        self.log(f"Lỗi khi xử lý truyện: {str(e)}", level="error", emit_log=emit_log)
                
                self.log(f"Đã tìm thấy {len(stories)} truyện trên trang {page}", emit_log=emit_log)
                
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

            story["author"] = self.get_text_safe(driver, "li.author.row p.col-xs-9 a")
            story["status"] = self.get_text_safe(driver, "li.status.row p.col-xs-9")
            story["description"] = self.get_text_safe(driver, "div.story-detail-info.detail-content")

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
            max_comment_pages = 3
            
            while page_comment <= max_comment_pages:
                try:
                    driver.execute_script("if (typeof loadComment === 'function') { loadComment(arguments[0]); }", page_comment)
                    time.sleep(random.uniform(1, 2))
                    
                    comment_elements = driver.find_elements(By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment")

                    if not comment_elements:
                        break
                        
                    for comment_elem in comment_elements:
                        username = self.get_text_safe(comment_elem, "div.outsite-comment div.outline-content-comment div:nth-child(1) strong")
                        content = self.get_text_safe(comment_elem, "div.outsite-comment div.outline-content-comment div.content-comment")

                        comments.append({
                            "username": username or "Ẩn danh",
                            "content": content,
                            "date": datetime.now()
                        })
                        
                    page_comment += 1
                    
                except Exception as e:
                    self.log(f"Lỗi khi tải trang bình luận {page_comment}: {str(e)}", level="error", emit_log=emit_log)
                    break
                    
            self.log(f"Đã lấy {len(comments)} bình luận", emit_log=emit_log)
                
        except Exception as e:
            self.log(f"Lỗi khi lấy bình luận: {str(e)}", level="error", emit_log=emit_log)
        finally:
            driver.quit()
            
        return comments

    def get_text_safe(self, driver, selector):
        """Hàm lấy text từ selector, tránh lỗi không tìm thấy"""
        try:
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except:
            return "N/A"

# Chạy thử crawler
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    crawler = TruyenQQCrawler(chromedriver_path=r"C:\Users\Hi\rating_comic\code\RatingComic\crawlers\chromedriver.exe")

    print("Đang lấy danh sách truyện...")
    stories = crawler.get_all_stories(num_pages=1)
    print(f"Đã lấy {len(stories)} truyện.")

    if stories:
        story = stories[0]
        print(f"Đang lấy chi tiết truyện: {story['title']}")
        detailed_story = crawler.get_story_details(story)
        print("Thông tin chi tiết:", detailed_story)

        print(f"Đang lấy bình luận cho truyện: {story['title']}")
        comments = crawler.get_comments(story)
        print(f"Đã lấy {len(comments)} bình luận.")
