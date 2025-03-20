import concurrent.futures
import sqlite3
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import random
import logging
import re
import threading

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Đường dẫn đến ChromeDriver
CHROMEDRIVER_PATH = r"C:\Users\Hi\rating_comic\Tasks 1\chromedriver.exe"
# Số luồng xử lý
WORKER_COUNT = 5
# Tên file SQLite database
DB_FILE = "truyenqq_data.db"
# Thread-local storage để lưu kết nối database cho từng thread
local_storage = threading.local()

def create_chrome_driver():
    """Tạo và cấu hình một phiên bản Chrome WebDriver"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Thêm user-agent để tránh bị chặn
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    return webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)

def get_text_safe(element, selector):
    """Trích xuất nội dung văn bản an toàn từ một phần tử sử dụng bộ chọn CSS"""
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except:
        return "N/A"

def extract_chapter_number(chapter_text):
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

def get_db_connection():
    """Trả về kết nối cơ sở dữ liệu dành riêng cho mỗi thread"""
    # Kiểm tra xem thread hiện tại đã có kết nối chưa
    if not hasattr(local_storage, 'conn'):
        # Tạo kết nối mới cho thread hiện tại
        local_storage.conn = sqlite3.connect(DB_FILE)
        logger.debug(f"Đã tạo kết nối mới cho thread ID: {threading.get_ident()}")
    return local_storage.conn

def init_database():
    """Khởi tạo cơ sở dữ liệu SQLite và tạo các bảng cần thiết"""
    # Kiểm tra và xóa file cũ nếu tồn tại để tránh trùng lặp dữ liệu
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Tạo bảng lưu thông tin truyện
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ten_truyen TEXT NOT NULL,
        link_truyen TEXT NOT NULL UNIQUE,
        ten_khac TEXT,
        tac_gia TEXT,
        trang_thai TEXT,
        luot_thich TEXT,
        luot_theo_doi TEXT,
        luot_xem TEXT,
        mo_ta TEXT,
        so_chuong INTEGER DEFAULT 0,
        so_binh_luan INTEGER DEFAULT 0
    )
    ''')
    
    # Tạo bảng lưu thông tin bình luận
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        story_id INTEGER,
        ten_nguoi_binh_luan TEXT,
        noi_dung_binh_luan TEXT,
        FOREIGN KEY (story_id) REFERENCES stories (id)
    )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("Đã khởi tạo cơ sở dữ liệu SQLite")
    return True

def get_all_stories():
    """Lấy danh sách truyện từ tất cả các trang bằng vòng lặp while"""
    all_stories = []
    page_num = 1
    has_more_pages = True
    
    # Lặp qua từng trang cho đến khi không còn trang mới hoặc gặp lỗi
    while True:
        driver = create_chrome_driver()
        try:
            url = f"https://truyenqqto.com/truyen-moi-cap-nhat/trang-{page_num}.html"
            logger.info(f"Đang lấy dữ liệu từ trang {page_num}")
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
                    has_more_pages = False
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
                    chapter_count = extract_chapter_number(chapter_info)
                    
                    page_stories.append({
                        "Tên truyện": story_name,
                        "Link truyện": story_link,
                        "Số chương": chapter_count
                    })
                
                logger.info(f"Trang {page_num}: Đã tìm thấy {len(page_stories)} truyện")
                all_stories.extend(page_stories)
                
                # Chuyển sang trang tiếp theo
                page_num += 1
                
            except Exception as e:
                logger.warning(f"Không thể lấy dữ liệu từ trang {page_num}: {str(e)}")
                has_more_pages = False  # Gặp lỗi, dừng vòng lặp
                
        except Exception as e:
            logger.error(f"Lỗi khi truy cập trang {page_num}: {str(e)}")
            has_more_pages = False  # Gặp lỗi, dừng vòng lặp
            
        finally:
            driver.quit()
            # Nghỉ ngẫu nhiên giữa các yêu cầu để tránh bị chặn
            time.sleep(random.uniform(1, 3))
    
    logger.info(f"Đã hoàn thành việc thu thập truyện từ {page_num-1} trang. Tổng cộng: {len(all_stories)} truyện")
    return all_stories

def get_story_details(story):
    """Lấy thông tin chi tiết về một truyện"""
    driver = create_chrome_driver()
    
    try:
        driver.get(story["Link truyện"])
        time.sleep(1)
        
        # Kiểm tra xem có phần tử tên khác không
        ten_khac_element = driver.find_elements(By.CSS_SELECTOR, "li.othername.row h2")
        if ten_khac_element:
            story["Tên khác"] = ten_khac_element[0].text.strip()
            story["Tác giả"] = get_text_safe(driver, "li.author.row p.col-xs-9 a")
            story["Trạng thái"] = get_text_safe(driver, "li.status.row p.col-xs-9")
            story["Lượt thích"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9.number-like")
            story["Lượt theo dõi"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
            story["Lượt xem"] = get_text_safe(driver, "li:nth-child(6) p.col-xs-9")
        else:
            story["Tên khác"] = "Không có tên khác"
            story["Tác giả"] = get_text_safe(driver, "li.author.row p.col-xs-9 a")
            story["Trạng thái"] = get_text_safe(driver, "li.status.row p.col-xs-9")
            story["Lượt thích"] = get_text_safe(driver, "li:nth-child(3) p.col-xs-9.number-like")
            story["Lượt theo dõi"] = get_text_safe(driver, "li:nth-child(4) p.col-xs-9")
            story["Lượt xem"] = get_text_safe(driver, "li:nth-child(5) p.col-xs-9")
        
        # Cố gắng nhấp vào "Xem thêm" nếu có
        try:
            readmore_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "p > a"))
            )
            readmore_button.click()
            time.sleep(1)
        except:
            pass
        
        story["Mô tả"] = get_text_safe(driver, "div.story-detail-info.detail-content")
        
    except Exception as e:
        logger.error(f"Lỗi khi lấy chi tiết cho '{story['Tên truyện']}': {str(e)}")
    finally:
        driver.quit()
        
    return story

def get_comments(story):
    """Lấy bình luận cho một truyện"""
    driver = None
    all_comments = []
    
    try:
        driver = create_chrome_driver()
        driver.get(story["Link truyện"])
        time.sleep(1)
        
        page_comment = 1
        while True:
            try:
                driver.execute_script(
                    "if (typeof loadComment === 'function') { loadComment(arguments[0]); } else { throw 'loadComment not found'; }", 
                    page_comment
                )
            except Exception:
                break
                
            time.sleep(random.uniform(1, 2))
            
            try:
                comments = WebDriverWait(driver, 5).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#comment_list .list-comment article.info-comment"))
                )
            except:
                break
                
            if not comments:
                break
                
            for comment in comments:
                name = get_text_safe(comment, "div.outsite-comment div.outline-content-comment div:nth-child(1) strong")
                content = get_text_safe(comment, "div.outsite-comment div.outline-content-comment div.content-comment")
                
                # Đảm bảo nội dung bình luận không để trống
                if not content or content.strip() == "":
                    content = "N/A"
                    
                # Đảm bảo tên người bình luận không để trống
                if not name or name.strip() == "":
                    name = "N/A"
                    
                all_comments.append({
                    "Tên truyện": story["Tên truyện"], 
                    "Tên người bình luận": name, 
                    "Nội dung bình luận": content
                })
                
            page_comment += 1
            
    except Exception as e:
        logger.error(f"Lỗi khi lấy bình luận cho '{story['Tên truyện']}': {str(e)}")
    finally:
        if driver:
            driver.quit()
    
    # Thêm số lượng bình luận vào story
    story["Số bình luận"] = len(all_comments)
    
    return all_comments

def save_story_to_db(story):
    """Lưu thông tin chi tiết của truyện vào cơ sở dữ liệu"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
        INSERT INTO stories (
            ten_truyen, link_truyen, ten_khac, tac_gia, trang_thai, 
            luot_thich, luot_theo_doi, luot_xem, mo_ta, so_chuong, so_binh_luan
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            story.get("Tên truyện", "N/A"),
            story.get("Link truyện", "N/A"),
            story.get("Tên khác", "N/A"),
            story.get("Tác giả", "N/A"),
            story.get("Trạng thái", "N/A"),
            story.get("Lượt thích", "N/A"),
            story.get("Lượt theo dõi", "N/A"),
            story.get("Lượt xem", "N/A"),
            story.get("Mô tả", "N/A"),
            story.get("Số chương", 0),
            story.get("Số bình luận", 0)
        ))
        
        conn.commit()
        return cursor.lastrowid
    except sqlite3.IntegrityError:
        # Nếu đã tồn tại link truyện (do unique constraint), lấy ID của truyện
        cursor.execute("SELECT id FROM stories WHERE link_truyen = ?", (story["Link truyện"],))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Lỗi khi lưu truyện '{story['Tên truyện']}' vào DB: {str(e)}")
        return None

def save_comments_to_db(story_id, comments):
    """Lưu các bình luận vào cơ sở dữ liệu"""
    if not comments or story_id is None:
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for comment in comments:
            cursor.execute('''
            INSERT INTO comments (story_id, ten_nguoi_binh_luan, noi_dung_binh_luan)
            VALUES (?, ?, ?)
            ''', (
                story_id,
                comment.get("Tên người bình luận", "N/A"),
                comment.get("Nội dung bình luận", "N/A")
            ))
            
        conn.commit()
    except Exception as e:
        logger.error(f"Lỗi khi lưu bình luận vào DB: {str(e)}")

def process_story(story):
    """Xử lý một truyện - lấy chi tiết và bình luận, lưu vào DB"""
    detailed_story = get_story_details(story)
    comments = get_comments(story)
    
    # Lưu vào SQLite (sử dụng kết nối riêng cho thread hiện tại)
    story_id = save_story_to_db(detailed_story)
    save_comments_to_db(story_id, comments)
    
    return detailed_story, comments

def collect_db_statistics():
    """Thu thập thống kê từ database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM stories")
        story_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM comments")
        comment_count = cursor.fetchone()[0]
        
        return story_count, comment_count
    finally:
        conn.close()

def main():
    """Hàm chính để điều phối quá trình thu thập dữ liệu"""
    # Khởi tạo cơ sở dữ liệu
    init_database()
    
    try:
        # Lấy tất cả truyện bằng vòng lặp while
        stories = get_all_stories()
        
        if not stories:
            logger.error("Không tìm thấy truyện nào. Thoát chương trình.")
            return
        
        # Xử lý từng truyện song song
        detailed_stories = []
        all_comments = []
        
        logger.info(f"Đang xử lý chi tiết và bình luận cho {len(stories)} truyện")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_COUNT) as executor:
            # Mỗi thread sẽ tạo kết nối riêng thay vì chia sẻ kết nối
            futures = [executor.submit(process_story, story) for story in stories]
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    detailed_story, comments = future.result()
                    detailed_stories.append(detailed_story)
                    all_comments.extend(comments)
                    
                    # Chỉ in tiến trình, không lưu từng đợt
                    if (i + 1) % 5 == 0:
                        logger.info(f"Đã xử lý {i + 1}/{len(stories)} truyện")
                    
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý truyện: {str(e)}")
        
        # Lưu toàn bộ dữ liệu vào file kết quả cuối cùng (Excel)
        df_stories = pd.DataFrame(detailed_stories)
        df_comments = pd.DataFrame(all_comments)
        
        excel_filename = "truyenqq_data.xlsx"
        with pd.ExcelWriter(excel_filename, engine="openpyxl") as writer:
            df_stories.to_excel(writer, sheet_name="Danh sách truyện", index=False)
            df_comments.to_excel(writer, sheet_name="Bình luận", index=False)
        
        logger.info(f"Toàn bộ dữ liệu đã được lưu vào {excel_filename}")
        logger.info(f"Toàn bộ dữ liệu đã được lưu vào cơ sở dữ liệu SQLite: {DB_FILE}")
        
        # Hiển thị thống kê từ cơ sở dữ liệu
        story_count, comment_count = collect_db_statistics()
        logger.info(f"Thống kê từ cơ sở dữ liệu: {story_count} truyện và {comment_count} bình luận")
        
    except Exception as e:
        logger.error(f"Lỗi nghiêm trọng: {str(e)}")

if __name__ == "__main__":
    main()