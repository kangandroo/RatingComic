from crawlers.base_crawler import BaseCrawler
from crawlers.truyenqq import TruyenQQCrawler
import json

def main():
    # Khởi tạo TruyenQQCrawler
    crawler = TruyenQQCrawler()
    
    # Lấy danh sách truyện
    print("Đang lấy danh sách truyện...")
    stories = crawler.get_all_stories(num_pages=1)  # Crawl 1 trang để test
    
    if not stories:
        print("Không lấy được danh sách truyện!")
        return
    
    # Hiển thị danh sách truyện
    print(f"Lấy được {len(stories)} truyện. Hiển thị thông tin truyện đầu tiên:")
    print(json.dumps(stories[0], indent=4, ensure_ascii=False))
    
    # Lấy thông tin chi tiết của truyện đầu tiên
    first_story = stories[0]
    print("\nĐang lấy chi tiết truyện...")
    detailed_story = crawler.get_story_details(first_story)
    print(json.dumps(detailed_story, indent=4, ensure_ascii=False))
    
    # Lấy bình luận của truyện đầu tiên
    print("\nĐang lấy bình luận...")
    comments = crawler.get_comments(first_story)
    print(f"Lấy được {len(comments)} bình luận.")
    if comments:
        print(json.dumps(comments[:3], indent=4, ensure_ascii=False))  # Hiển thị 3 bình luận đầu tiên
    
if __name__ == "__main__":
    main()
