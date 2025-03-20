import os
import time
import random
import sqlite3
import logging
import threading
import concurrent.futures
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging - minimal console output
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s'
                    )
logger = logging.getLogger()

# Đường dẫn đến ChromeDriver
CHROMEDRIVER_PATH = r"C:\Users\Hi\rating_comic\Tasks 1\chromedriver.exe"

# Excel filename
EXCEL_FILE = "nettruyen_data.xlsx"

# Thread-local storage for database connections
thread_local = threading.local()

# Counter for completed stories and lock for thread safety
completed_stories = 0
completed_lock = threading.Lock()
total_stories = 0

# Database functions
def init_database():
    """Khởi tạo cơ sở dữ liệu và các bảng cần thiết"""
    conn = sqlite3.connect('nettruyen_data.db')
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    
    # Tạo bảng truyện
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        link TEXT UNIQUE,
        author TEXT,
        status TEXT,
        rating TEXT,
        followers INTEGER,
        views INTEGER,
        rating_count INTEGER,
        chapter_count INTEGER,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Tạo bảng bình luận
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        story_id INTEGER,
        user_name TEXT,
        content TEXT,
        story_title TEXT,
        crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (story_id) REFERENCES stories (id)
    )
    ''')
    
    conn.commit()
    conn.close()

def get_db_connection():
    """Lấy kết nối database cho thread hiện tại"""
    if not hasattr(thread_local, "connection"):
        thread_local.connection = sqlite3.connect('nettruyen_data.db')
        thread_local.connection.execute("PRAGMA foreign_keys = ON")
    return thread_local.connection

def save_story_to_db(story):
    """Lưu thông tin truyện vào database (thread-safe)"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Chuyển đổi dữ liệu số
        followers = extract_number(story.get('Lượt theo dõi', '0'))
        views = extract_number(story.get('Lượt xem', '0'))
        rating_count = extract_number(story.get('Lượt đánh giá', '0'))
        chapter_count = story.get('Số chương', 0)
        
        cursor.execute('''
        INSERT OR REPLACE INTO stories 
        (title, link, author, status, rating, followers, views, rating_count, chapter_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            story.get('Tên truyện', 'Unknown'),
            story.get('Link truyện', ''),
            story.get('Tác giả', 'N/A'),
            story.get('Trạng thái', 'N/A'),
            story.get('Đánh giá', 'N/A'),
            followers,
            views,
            rating_count,
            chapter_count
        ))
        
        conn.commit()
        return cursor.lastrowid
    except Exception as e:
        logger.error(f"Lỗi khi lưu truyện vào DB: {e}")
        return None

def save_comments_to_db(story_id, story_title, comments):
    """Lưu danh sách bình luận vào database (thread-safe)"""
    if not story_id or not comments:
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        for comment in comments:
            cursor.execute('''
            INSERT INTO comments (story_id, user_name, content, story_title)
            VALUES (?, ?, ?, ?)
            ''', (
                story_id,
                comment.get('Tên người bình luận', 'Anonymous'),
                comment.get('Nội dung bình luận', 'N/A'),
                story_title
            ))
        conn.commit()
    except Exception as e:
        logger.error(f"Lỗi khi lưu bình luận vào DB: {e}")

def extract_number(text_value):
    """
    Trích xuất số từ các chuỗi với nhiều định dạng như:
    - '1,234' -> 1234
    - '2.345.737' -> 2345737
    - '5K' -> 5000
    - '3.2M' -> 3200000
    """
    try:
        # Xử lý trường hợp đã là số
        if isinstance(text_value, (int, float)):
            return int(text_value)
            
        # Xử lý giá trị trống hoặc không có
        if not text_value or text_value == 'N/A':
            return 0
            
        text_value = str(text_value).strip()
        
        # Xử lý hậu tố K và M
        if 'K' in text_value.upper():
            num_part = text_value.upper().replace('K', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                # Xử lý trường hợp như "5.2K"
                return int(float(num_part) * 1000)
            else:
                # Xử lý trường hợp như "5.2.K" hoặc "5K"
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000)
            
        elif 'M' in text_value.upper():
            num_part = text_value.upper().replace('M', '')
            # Làm sạch và chuyển đổi
            if num_part.count('.') == 1:
                # Xử lý trường hợp như "3.2M"
                return int(float(num_part) * 1000000)
            else:
                # Xử lý trường hợp như "3.2.M" hoặc "3M"
                cleaned = num_part.replace('.', '').replace(',', '')
                return int(float(cleaned) * 1000000)
        else:
            # Xử lý số có nhiều dấu chấm là dấu phân cách hàng nghìn như "2.345.737"
            if text_value.count('.') > 1:
                text_value = text_value.replace('.', '')
            
            # Xử lý dấu phẩy là dấu phân cách hàng nghìn
            text_value = text_value.replace(',', '')
            
            return int(float(text_value))
    except Exception as e:
        import logging
        logging.error(f"Lỗi khi trích xuất số từ '{text_value}': {e}")
        return 0

# **Hàm lấy dữ liệu an toàn**
def get_text_safe(element, selector):
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except Exception:
        return "N/A"

# **Hàm khởi tạo trình duyệt Chrome**
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=vi")
    return webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)

# **Hàm lấy danh sách truyện trên nhiều trang**
def get_all_stories(max_pages=None):
    driver = setup_driver()
    stories = []
    page = 1
    
    try:
        while True:
            url = f"https://nettruyenvie.com/?page={page}"
            driver.get(url)
            time.sleep(random.uniform(2, 4))

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".items .row .item"))
                )
            except Exception:
                break

            # Lấy tất cả các item truyện
            item_elements = driver.find_elements(By.CSS_SELECTOR, ".items .row .item")
            
            if not item_elements:
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
                    chapter_count = extract_chapter_number(chapter_info)
                    
                    if link:
                        stories.append({
                            "Tên truyện": title, 
                            "Link truyện": link,
                            "Số chương": chapter_count
                        })
                except Exception:
                    continue

            # Kiểm tra có trang tiếp theo không
            next_buttons = driver.find_elements(By.CSS_SELECTOR, ".pagination a.next")
            if not next_buttons:
                break

            page += 1
            time.sleep(random.uniform(2, 3))
            
    except Exception as e:
        logger.error(f"Lỗi khi lấy danh sách truyện: {e}")
    finally:
        driver.quit()
        
    print(f"Đã tìm thấy {len(stories)} truyện để crawl")
    return stories

def extract_chapter_number(chapter_text):
    """Trích xuất số chương từ text (ví dụ: 'Chapter 124' -> 124)"""
    try:
        import re
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

# **Hàm lấy thông tin chi tiết truyện**
def get_story_details(story):
    driver = setup_driver()
    
    try:
        driver.get(story["Link truyện"])
        time.sleep(random.uniform(2, 3))

        story["Tác giả"] = get_text_safe(driver, "li.author.row p.col-xs-8")
        story["Trạng thái"] = get_text_safe(driver, "li.status.row p.col-xs-8")
        story["Đánh giá"] = get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(1)")
        story["Lượt theo dõi"] = int(get_text_safe(driver, ".follow span b.number_follow").replace(".", ""))
        story["Lượt xem"] = int(get_text_safe(driver, "ul.list-info li:last-child p.col-xs-8").replace(".", ""))
        story["Lượt đánh giá"] = get_text_safe(driver, ".mrt5.mrb10 span span:nth-child(3)")

        # Cố gắng lấy số chương chính xác từ thông tin chi tiết nếu có thể
        try:
            # Thử tìm chương mới nhất
            chapter_element = driver.find_element(By.CSS_SELECTOR, ".list-chapter li:first-child a")
            if chapter_element:
                chapter_text = chapter_element.get_attribute("title")
                if chapter_text:
                    chapter_count = extract_chapter_number(chapter_text)
                    if chapter_count > 0:
                        story["Số chương"] = chapter_count
                    
            # Nếu không tìm thấy số chương hoặc số chương = 0, thử đếm các chương
            if story.get("Số chương", 0) == 0:
                chapter_items = driver.find_elements(By.CSS_SELECTOR, ".list-chapter li")
                story["Số chương"] = len(chapter_items)
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Lỗi khi lấy thông tin chi tiết truyện {story.get('Tên truyện')}: {e}")
    finally:
        driver.quit()
        
    return story

# **Hàm lấy bình luận của truyện**
def get_comments(story):
    driver = setup_driver()
    comments = []
    unique_contents = set()
    duplicate_found = False
    
    try:
        driver.get(story["Link truyện"])
        time.sleep(random.uniform(2, 3))

        # Gọi hàm joinComment() để chuyển đến phần comment
        try:
            driver.execute_script("joinComment()")
            time.sleep(random.uniform(2, 3))
        except Exception:
            pass
            
        page_comment = 1
        max_comment_pages = 1000000  # Giới hạn số trang bình luận
        
        while page_comment <= max_comment_pages:
            comments_in_current_page = 0

            # Lấy danh sách bình luận với selector cụ thể
            try:
                comment_elements = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".info"))
                )
                
                if not comment_elements:
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
                        break
                    
                    # Nếu nội dung khác N/A, thêm vào tập hợp để kiểm tra trùng lặp sau này
                    if content != "N/A":
                        unique_contents.add(content)
                    
                    comments.append({
                        "Tên truyện": story["Tên truyện"], 
                        "Tên người bình luận": name, 
                        "Nội dung bình luận": content
                    })
                    comments_in_current_page += 1
                
                # Nếu phát hiện bình luận trùng lặp, dừng việc chuyển trang
                if duplicate_found:
                    break
                
                # Thêm điều kiện dừng: Nếu số lượng bình luận trong trang < 10
                if comments_in_current_page < 10:
                    break
                    
            except Exception:
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
                    time.sleep(random.uniform(2, 3))
                else:
                    break
            except Exception:
                break
        
    except Exception:
        pass
    finally:
        driver.quit()
        
    return comments

# **Hàm xử lý mỗi truyện (đa luồng)**
def process_story(story):
    global completed_stories
    try:
        detailed_story = get_story_details(story)
        comments = get_comments(story)
        
        # Lưu vào cơ sở dữ liệu
        story_id = save_story_to_db(detailed_story)
        if story_id:
            save_comments_to_db(story_id, detailed_story["Tên truyện"], comments)
        
        # Cập nhật biến đếm số truyện đã hoàn thành (thread-safe)
        with completed_lock:
            completed_stories += 1
            current_completed = completed_stories
        
        # Thêm print statement để hiển thị truyện đã hoàn thành và tiến độ
        print(f"Đã hoàn thành truyện: {detailed_story.get('Tên truyện')} - {len(comments)} bình luận - {detailed_story.get('Số chương', 0)} chương ({current_completed}/{total_stories})")
        
        return detailed_story, comments
    except Exception as e:
        logger.error(f"Lỗi khi xử lý truyện {story.get('Tên truyện')}: {e}")
        
        # Vẫn phải cập nhật số lượng hoàn thành kể cả khi có lỗi
        with completed_lock:
            completed_stories += 1
            current_completed = completed_stories
            
        print(f"Lỗi khi xử lý truyện: {story.get('Tên truyện')} ({current_completed}/{total_stories})")
        
        return story, []

# **Hàm xuất dữ liệu từ SQLite sang Excel**
def export_to_excel():
    """Xuất dữ liệu từ cơ sở dữ liệu SQLite sang file Excel"""
    try:
        conn = sqlite3.connect('nettruyen_data.db')
        
        # Truy vấn danh sách truyện
        stories_df = pd.read_sql_query('''
            SELECT 
                title as "Tên truyện",
                author as "Tác giả", 
                status as "Trạng thái", 
                rating as "Đánh giá",
                followers as "Lượt theo dõi", 
                views as "Lượt xem",
                rating_count as "Lượt đánh giá",
                chapter_count as "Số chương"
            FROM stories
        ''', conn)
        
        # Truy vấn danh sách bình luận
        comments_df = pd.read_sql_query('''
            SELECT 
                story_title as "Tên truyện",
                user_name as "Tên người bình luận",
                content as "Nội dung bình luận"
            FROM comments
        ''', conn)
        
        # Xuất ra file Excel với 2 sheet
        with pd.ExcelWriter(EXCEL_FILE, engine='openpyxl') as writer:
            stories_df.to_excel(writer, sheet_name='Danh sách truyện', index=False)
            comments_df.to_excel(writer, sheet_name='Bình luận', index=False)
        
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Lỗi khi xuất dữ liệu sang Excel: {e}")
        return False

# **Hàm chính**
def main():
    global total_stories
    try:
        print("Bắt đầu quá trình crawl dữ liệu từ NetTruyen...")
        
        # Khởi tạo cơ sở dữ liệu
        init_database()
        
        # Lấy danh sách truyện
        stories_list = get_all_stories(max_pages=20)  # Điều chỉnh số trang ở đây
        
        if not stories_list:
            print("Không tìm thấy truyện nào!")
            return
            
        # Cập nhật tổng số truyện cần xử lý
        total_stories = len(stories_list)
        
        # Sử dụng đa luồng để xử lý song song
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_story, story) for story in stories_list]
            
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Chỉ để đảm bảo nắm bắt lỗi nếu có
        
        # Xuất dữ liệu ra file Excel
        export_to_excel()
        
        # Thêm print statement để hiển thị thông báo hoàn thành
        print(f"\nĐã hoàn thành {completed_stories}/{total_stories} truyện")
        print(f"Dữ liệu đã được lưu vào:")
        print(f"1. SQLite Database: nettruyen_data.db")
        print(f"2. Excel File: {EXCEL_FILE}")
        
    except Exception as e:
        logger.error(f"Đã xảy ra lỗi trong quá trình crawl: {e}")
        print(f"Đã xảy ra lỗi: {e}")

if __name__ == "__main__":
    main()