import concurrent.futures
import sqlite3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
import random
import re

CHROMEDRIVER_PATH = r"C:\Users\Hi\rating_comic\crawl\chromedriver.exe"
DB_PATH = "manhuavn.db"

# Hàm tạo bảng trong SQLite
def create_tables():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS stories (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT,
                        link TEXT,
                        status TEXT,
                        followers TEXT,
                        views TEXT,
                        rating TEXT,
                        rating_count TEXT,
                        description TEXT,
                        chapters TEXT,
                        author TEXT
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS comments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        story_title TEXT,
                        user TEXT,
                        content TEXT,
                        FOREIGN KEY(story_title) REFERENCES stories(title)
                    )''')
    conn.commit()
    conn.close()

# Hàm chèn dữ liệu vào bảng stories
def insert_story(story):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''INSERT INTO stories (title, link, status, followers, views, rating, rating_count, description, chapters, author)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (story["Tên truyện"], story["Link truyện"], story["Tình trạng"], story["Lượt theo dõi"], story["Lượt xem"], 
                    story["Đánh giá"], story["Lượt đánh giá"], story["Mô tả"], story["Số chương"], story["Tác giả"]))
    conn.commit()
    conn.close()

# Hàm chèn dữ liệu vào bảng comments
def insert_comments(comments):
    if not comments:
        return
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany('''INSERT INTO comments (story_title, user, content)
                          VALUES (?, ?, ?)''',
                       [(comment["Tên truyện"], comment["Người bình luận"], comment["Nội dung bình luận"]) for comment in comments])
    conn.commit()
    conn.close()

# Hàm lấy dữ liệu an toàn
def get_text_safe(element, selector):
    try:
        return element.find_element(By.CSS_SELECTOR, selector).text.strip()
    except:
        return "N/A"

# Hàm lấy danh sách truyện
def get_all_stories():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)
    
    stories = []
    page = 1

    while True:
        print(f"\n📌 Đang lấy danh sách truyện - Trang {page}...")
        url = f"https://manhuavn.top/danhsach/P{page}/index.html?status=0&sort=2"
        driver.get(url)
        time.sleep(2)

        try:
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".lst_story .story_item")))
        except:
            print("❌ Không tìm thấy truyện! Kết thúc.")
            break

        elems = driver.find_elements(By.CSS_SELECTOR, ".lst_story .story_item")
        if not elems:
            print("❌ Không còn truyện nào!")
            break

        for elem in elems:
            title = get_text_safe(elem, ".story_title")
            link = elem.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
            stories.append({"Tên truyện": title, "Link truyện": link})

        page += 1

    driver.quit()
    return stories

# HÀM 1: Lấy thông tin chi tiết của truyện
def get_story_qualitative_data(story):
    """
    Lấy thông tin chi tiết của truyện như tình trạng, lượt theo dõi, lượt xem, đánh giá, mô tả, số chương và tác giả.
    
    Args:
        story: Dictionary chứa thông tin cơ bản của truyện (tên và link)
        
    Returns:
        Dictionary chứa thông tin chi tiết của truyện
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)

    story_data = story.copy()  # Tạo bản sao để không ảnh hưởng đến dữ liệu gốc
    
    try:
        print(f"Đang lấy thông tin chi tiết cho truyện: {story['Tên truyện']}")
        driver.get(story["Link truyện"])
        time.sleep(2)

        # Lấy thông tin chi tiết
        story_data["Tình trạng"] = get_text_safe(driver, ".info-row .contiep")
        story_data["Lượt theo dõi"] = get_text_safe(driver, "li.info-row strong")
        story_data["Lượt xem"] = get_text_safe(driver, "li.info-row view.colorblue")
        story_data["Đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingValue"]')
        story_data["Lượt đánh giá"] = get_text_safe(driver, 'span[itemprop="ratingCount"]')
        story_data["Mô tả"] = get_text_safe(driver, "li.clearfix p")

        chapter_text = get_text_safe(driver, "li.info-row a.colorblue")
        story_data["Số chương"] = re.search(r'\d+', chapter_text).group() if chapter_text != "N/A" and re.search(r'\d+', chapter_text) else "Không tìm thấy"
        
        # Lấy tác giả
        try:
            author_element = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[1]/div[1]/div[3]/ul/li[6]/a")
            story_data["Tác giả"] = author_element.text.strip()
        except:
            story_data["Tác giả"] = "N/A"

    except Exception as e:
        print(f"Lỗi khi lấy thông tin chi tiết cho '{story['Tên truyện']}': {str(e)}")
    finally:
        driver.quit()
    
    return story_data

# HÀM 2: Lấy bình luận của truyện
def get_story_comments(story):
    """
    Lấy tất cả bình luận của truyện
    
    Args:
        story: Dictionary chứa thông tin cơ bản của truyện (tên và link)
        
    Returns:
        List chứa các bình luận của truyện
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)

    comments_data = []
    
    try:
        print(f"Đang lấy bình luận cho truyện: {story['Tên truyện']}")
        driver.get(story["Link truyện"])
        time.sleep(2)

        # Mở và lấy bình luận
        # Hàm mở tất cả bình luận
        while True:
            try:
                load_more_button = driver.find_element(By.XPATH, "/html/body/div[2]/div[2]/div/div[1]/div[4]/div[2]/div[1]/div[4]/ul/div/button")
                load_more_button.click()
                time.sleep(2)
            except:
                break

        comments = driver.find_elements(By.CSS_SELECTOR, "li.comment_item")
        for comment in comments:
            user = get_text_safe(comment, ".comment-head")
            content = get_text_safe(comment, ".comment-content")
            comments_data.append({
                "Tên truyện": story["Tên truyện"], 
                "Người bình luận": user, 
                "Nội dung bình luận": content
            })

    except Exception as e:
        print(f"Lỗi khi lấy bình luận cho '{story['Tên truyện']}': {str(e)}")
    finally:
        driver.quit()
    
    return comments_data

# Hàm kết hợp lấy cả thông tin chi tiết và bình luận
def get_story_details(story):
    # Lấy thông tin chi tiết
    detailed_story = get_story_qualitative_data(story)
    
    # Lấy bình luận
    comments = get_story_comments(story)
    
    return detailed_story, comments

# Chạy chính
def main():
    try:
        create_tables()
        all_stories = get_all_stories()
        if all_stories:
            print(f"Đã tìm thấy {len(all_stories)} truyện")

            detailed_stories = []
            all_comments = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(get_story_details, all_stories))
                
            for details, comments in results:
                detailed_stories.append(details)
                all_comments.extend(comments)
                insert_story(details)
                insert_comments(comments)

            # Lưu vào file Excel với 2 sheet
            with pd.ExcelWriter("manhuavn_data.xlsx", engine="openpyxl") as writer:
                pd.DataFrame(detailed_stories).to_excel(writer, sheet_name="Danh sách truyện", index=False)
                pd.DataFrame(all_comments).to_excel(writer, sheet_name="Bình luận", index=False)
            
            print("✅ Đã lưu thông tin truyện và bình luận vào cơ sở dữ liệu SQLite và file manhuavn_data.xlsx")

        else:
            print("❌ Không tìm thấy truyện nào để lấy chi tiết!")

    except Exception as e:
        print(f"Lỗi trong quá trình chạy: {str(e)}")

if __name__ == "__main__":
    main()