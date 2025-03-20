from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, send_file
from flask_socketio import SocketIO
import os
import time
import pandas as pd
import concurrent.futures
from datetime import datetime
import json
import logging

# Import các module từ project
from db.models import db, Website, Story, Comment, AnalysisReport
from crawlers.nettruyen import NetTruyenCrawler
from crawlers.truyenqq import TruyenQQCrawler
from crawlers.manhuavn import ManhuaVNCrawler
from analyzers.sentiment import SentimentAnalyzer
from analyzers.rating import ComicRatingCalculator
from utils.exporter import ExcelExporter

# Thiết lập logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Khởi tạo Flask app
app = Flask(__name__)
app.config.from_object('config.Config')
app.secret_key = app.config['SECRET_KEY']

# Khởi tạo database
db.init_app(app)

# Khởi tạo SocketIO
socketio = SocketIO(app, cors_allowed_origins="*")

# Khởi tạo các crawlers
crawlers = {
    'nettruyen': NetTruyenCrawler(logger),
    'truyenqq': TruyenQQCrawler(logger),
    'manhuavn': ManhuaVNCrawler(logger)
}

# Khởi tạo analyzer
sentiment_analyzer = SentimentAnalyzer(logger=logger)

# Tạo thư mục uploads nếu chưa tồn tại
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.before_request
def create_tables():
    """Tạo bảng và dữ liệu ban đầu"""
    with app.app_context():
        db.create_all()
        
        # Kiểm tra và thêm thông tin website nếu chưa có
        if Website.query.count() == 0:
            websites = [
                {"name": "NetTruyen", "url": "https://nettruyenvie.com", "api_name": "nettruyen"},
                {"name": "TruyenQQ", "url": "https://truyenqqto.com", "api_name": "truyenqq"},
                {"name": "ManhuaVN", "url": "https://manhuavn.top", "api_name": "manhuavn"}
            ]
            
            for site_data in websites:
                website = Website(
                    name=site_data["name"],
                    url=site_data["url"],
                    api_name=site_data["api_name"]
                )
                db.session.add(website)
                
            db.session.commit()
            logger.info("Đã khởi tạo dữ liệu website")

@app.route('/')
def index():
    """Trang chủ - Chọn website để crawl"""
    websites = Website.query.all()
    return render_template('index.html', websites=websites)

@app.route('/crawl', methods=['POST'])
def crawl():
    """Endpoint để crawl dữ liệu"""
    website_id = request.form.get('website')
    num_pages = int(request.form.get('pages', 3))
    
    # Lấy thông tin website
    website = Website.query.get(website_id)
    if not website:
        return jsonify({'status': 'error', 'message': 'Website không hợp lệ'})
    
    # Lưu thông tin vào session để có thể truy cập ở các route khác
    session_data = {
        'website_id': website_id,
        'website_name': website.name,
        'crawler': website.api_name,
        'num_pages': num_pages
    }
    
    return jsonify({'status': 'success', 'redirect': url_for('process_crawl'), 'data': session_data})

@app.route('/process_crawl')
def process_crawl():
    """Hiển thị trang xử lý crawl"""
    return render_template('process_crawl.html')

@socketio.on('start_crawl')
def handle_start_crawl(data):
    """Xử lý sự kiện bắt đầu crawl từ socket"""
    website_id = data.get('website_id')
    num_pages = data.get('num_pages', 3)
    crawler_name = data.get('crawler')
    
    # Kiểm tra dữ liệu hợp lệ
    if not website_id or not crawler_name or crawler_name not in crawlers:
        socketio.emit('log_message', "Dữ liệu không hợp lệ!")
        return
    
    website = Website.query.get(website_id)
    if not website:
        socketio.emit('log_message', "Website không tồn tại!")
        return
    
    # Lấy crawler phù hợp
    crawler = crawlers[crawler_name]
    
    # Hàm callback để gửi log qua socket
    def emit_log(message):
        socketio.emit('log_message', message)
    
    # Lấy danh sách truyện
    emit_log(f"Bắt đầu crawl từ {website.name}...")
    stories_data = crawler.get_all_stories(num_pages=num_pages, emit_log=emit_log)
    
    emit_log(f"Đã tìm thấy {len(stories_data)} truyện. Đang lấy thông tin chi tiết...")
    
    # Lưu thông tin truyện vào database
    for i, story_data in enumerate(stories_data):
        # Kiểm tra xem truyện đã tồn tại chưa
        existing_story = Story.query.filter_by(url=story_data['url']).first()
        if existing_story:
            emit_log(f"Truyện '{story_data['title']}' đã tồn tại. Bỏ qua.")
            continue
        
        # Lấy thông tin chi tiết
        try:
            emit_log(f"[{i+1}/{len(stories_data)}] Đang lấy thông tin chi tiết cho truyện: {story_data['title']}")
            detailed_story = crawler.get_story_details(story_data, emit_log=emit_log)
            
            # Tạo đối tượng Story
            genres_str = ",".join(detailed_story.get('genres', [])) if isinstance(detailed_story.get('genres', []), list) else str(detailed_story.get('genres', ''))
            
            story = Story(
                title=detailed_story['title'],
                alt_title=detailed_story.get('alt_title', ''),
                url=detailed_story['url'],
                cover_url=detailed_story.get('cover_url', ''),
                author=detailed_story.get('author', 'Đang cập nhật'),
                status=detailed_story.get('status', 'Đang cập nhật'),
                genres=genres_str,
                description=detailed_story.get('description', ''),
                views=detailed_story.get('views', 0),
                likes=detailed_story.get('likes', 0),
                follows=detailed_story.get('follows', 0),
                chapter_count=detailed_story.get('chapter_count', 0),
                rating=detailed_story.get('rating', 0),
                rating_count=detailed_story.get('rating_count', 0),
                website_id=website.id
            )
            
            # Lưu vào database
            db.session.add(story)
            db.session.commit()
            
            # Tính điểm cơ bản
            ratings = ComicRatingCalculator.calculate_comprehensive_rating(
                {
                    'views': story.views,
                    'likes': story.likes,
                    'follows': story.follows,
                    'chapter_count': story.chapter_count,
                    'rating': story.rating,
                    'rating_count': story.rating_count
                }, 
                source=crawler_name
            )
            
            # Cập nhật điểm cơ bản
            story.view_score = ratings['view_score']
            story.like_score = ratings.get('like_score', 0)
            story.follow_score = ratings['follow_score']
            story.chapter_score = ratings['chapter_score']
            story.rating_score = ratings.get('rating_score', 0)
            story.base_rating = ratings['base_rating']
            story.final_rating = ratings['base_rating']  # Chưa có phân tích sentiment
            
            db.session.commit()
            
        except Exception as e:
            emit_log(f"Lỗi khi xử lý truyện '{story_data['title']}': {str(e)}")
            logger.error(f"Lỗi khi xử lý truyện: {str(e)}")
            continue
    
    emit_log("Đã hoàn thành quá trình crawl!")
    socketio.emit('crawl_completed', {'redirect': url_for('stories_list')})

@app.route('/stories')
def stories_list():
    """Hiển thị danh sách truyện đã crawl"""
    # Lấy các tham số filter
    website_id = request.args.get('website', type=int)
    genre = request.args.get('genre', '')
    min_views = request.args.get('min_views', type=int)
    min_comments = request.args.get('min_comments', type=int)
    
    # Query cơ bản
    query = Story.query
    
    # Áp dụng các bộ lọc
    if website_id:
        query = query.filter_by(website_id=website_id)
    
    if genre:
        query = query.filter(Story.genres.like(f'%{genre}%'))
    
    if min_views:
        query = query.filter(Story.views >= min_views)
    
    # Filter by comments cần join với bảng Comment
    if min_comments:
        query = query.join(Comment).group_by(Story.id).having(db.func.count(Comment.id) >= min_comments)
    
    # Lấy kết quả
    stories = query.all()
    
    # Lấy danh sách website và thể loại cho bộ lọc
    websites = Website.query.all()
    
    # Trích xuất tất cả các thể loại từ truyện
    all_genres = set()
    for story in Story.query.all():
        if story.genres:
            genres = [g.strip() for g in story.genres.split(',')]
            all_genres.update(genres)
    
    # Sắp xếp thể loại
    genres = sorted(list(all_genres))
    
    return render_template('stories_list.html', 
                          stories=stories, 
                          websites=websites, 
                          genres=genres,
                          selected_filters={
                              'website_id': website_id,
                              'genre': genre,
                              'min_views': min_views,
                              'min_comments': min_comments
                          })

@app.route('/analyze', methods=['POST'])
def analyze_stories():
    """Phân tích sentiment cho các truyện đã chọn"""
    selected_story_ids = request.form.getlist('selected_stories')
    
    if not selected_story_ids:
        flash('Vui lòng chọn ít nhất một truyện để phân tích', 'warning')
        return redirect(url_for('stories_list'))
    
    # Lưu ID truyện đã chọn vào session
    session_data = {
        'selected_story_ids': selected_story_ids
    }
    
    return jsonify({'status': 'success', 'redirect': url_for('process_analyze'), 'data': session_data})

@app.route('/process_analyze')
def process_analyze():
    """Hiển thị trang xử lý phân tích"""
    return render_template('process_analyze.html')

@socketio.on('start_analyze')
def handle_start_analyze(data):
    """Xử lý sự kiện bắt đầu phân tích từ socket"""
    selected_story_ids = data.get('selected_story_ids', [])
    
    if not selected_story_ids:
        socketio.emit('log_message', "Không có truyện nào được chọn để phân tích!")
        return
    
    # Hàm callback để gửi log qua socket
    def emit_log(message):
        socketio.emit('log_message', message)
    
    # Xử lý từng truyện
    for i, story_id in enumerate(selected_story_ids):
        story = Story.query.get(story_id)
        if not story:
            emit_log(f"Không tìm thấy truyện với ID {story_id}. Bỏ qua.")
            continue
        
        emit_log(f"[{i+1}/{len(selected_story_ids)}] Đang phân tích truyện: {story.title}")
        
        # Lấy crawler phù hợp
        website = Website.query.get(story.website_id)
        if not website:
            emit_log(f"Không tìm thấy thông tin website cho truyện {story.title}. Bỏ qua.")
            continue
            
        crawler = crawlers.get(website.api_name)
        if not crawler:
            emit_log(f"Không có crawler cho website {website.name}. Bỏ qua.")
            continue
        
        # Kiểm tra xem đã phân tích chưa
        if story.has_analyzed_comments:
            emit_log(f"Truyện '{story.title}' đã được phân tích trước đó. Bỏ qua lấy comment.")
        else:
            # Lấy bình luận
            emit_log(f"Đang lấy bình luận cho truyện: {story.title}")
            
            comments_data = crawler.get_comments({'title': story.title, 'url': story.url}, emit_log=emit_log)
            
            if not comments_data:
                emit_log(f"Không tìm thấy bình luận nào cho truyện {story.title}")
                
                # Cập nhật trạng thái đã phân tích
                story.has_analyzed_comments = True
                db.session.commit()
                continue
            
            emit_log(f"Đã lấy {len(comments_data)} bình luận. Đang phân tích sentiment...")
            
            # Lưu bình luận vào database
            for comment_data in comments_data:
                # Phân tích sentiment
                sentiment_result = sentiment_analyzer.analyze(comment_data['content'])
                
                # Tạo đối tượng Comment
                comment = Comment(
                    story_id=story.id,
                    username=comment_data.get('username', 'Ẩn danh'),
                    content=comment_data['content'],
                    date=comment_data.get('date', datetime.now()),
                    sentiment=sentiment_result['sentiment'],
                    sentiment_score=sentiment_result['score']
                )
                
                db.session.add(comment)
            
            db.session.commit()
            
            # Tính toán các tỷ lệ sentiment
            all_comments = Comment.query.filter_by(story_id=story.id).all()
            total_comments = len(all_comments)
            
            if total_comments > 0:
                positive_comments = [c for c in all_comments if c.sentiment == 'positive']
                negative_comments = [c for c in all_comments if c.sentiment == 'negative']
                neutral_comments = [c for c in all_comments if c.sentiment == 'neutral']
                
                positive_ratio = len(positive_comments) / total_comments
                negative_ratio = len(negative_comments) / total_comments
                neutral_ratio = len(neutral_comments) / total_comments
                
                # Tính điểm sentiment
                sentiment_score = ComicRatingCalculator.calculate_sentiment_rating(
                    positive_ratio,
                    negative_ratio,
                    neutral_ratio
                )
                
                # Cập nhật thông tin story
                story.positive_ratio = positive_ratio
                story.negative_ratio = negative_ratio
                story.neutral_ratio = neutral_ratio
                story.sentiment_score = sentiment_score
                
                # Tính điểm tổng hợp
                ratings = ComicRatingCalculator.calculate_comprehensive_rating(
                    {
                        'views': story.views,
                        'likes': story.likes,
                        'follows': story.follows,
                        'chapter_count': story.chapter_count,
                        'rating': story.rating,
                        'rating_count': story.rating_count,
                        'sentiment_rating': sentiment_score,
                        'comment_count': total_comments
                    }, 
                    source=website.api_name
                )
                
                story.final_rating = ratings['final_rating']
            
            # Đánh dấu đã phân tích
            story.has_analyzed_comments = True
            db.session.commit()
            
            emit_log(f"Đã hoàn thành phân tích truyện: {story.title}")
    
    emit_log("Đã hoàn thành phân tích tất cả truyện!")
    socketio.emit('analyze_completed', {'redirect': url_for('analysis_results', story_ids=','.join(selected_story_ids))})

@app.route('/analysis_results')
def analysis_results():
    """Hiển thị kết quả phân tích"""
    story_ids_str = request.args.get('story_ids', '')
    
    if not story_ids_str:
        flash('Không có truyện nào được chọn', 'warning')
        return redirect(url_for('stories_list'))
    
    story_ids = [int(id) for id in story_ids_str.split(',') if id.isdigit()]
    stories = Story.query.filter(Story.id.in_(story_ids)).all()
    
    # Tính thống kê
    for story in stories:
        # Lấy 5 comment tích cực và 5 comment tiêu cực tiêu biểu
        story.top_positive = Comment.query.filter_by(story_id=story.id, sentiment='positive') \
                                  .order_by(Comment.sentiment_score.desc()).limit(5).all()
                                  
        story.top_negative = Comment.query.filter_by(story_id=story.id, sentiment='negative') \
                                  .order_by(Comment.sentiment_score.asc()).limit(5).all()
    
    return render_template('analysis.html', stories=stories)

@app.route('/export', methods=['POST'])
def export_analysis():
    """Xuất kết quả phân tích ra file Excel"""
    story_ids = request.form.getlist('story_ids')
    
    if not story_ids:
        flash('Không có truyện nào được chọn để xuất', 'warning')
        return redirect(url_for('stories_list'))
    
    # Lấy thông tin truyện
    stories = Story.query.filter(Story.id.in_(story_ids)).all()
    
    # Tạo file Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(app.config['UPLOAD_FOLDER'], f"analysis_{timestamp}")
    os.makedirs(output_dir, exist_ok=True)
    
    excel_path = ExcelExporter.export_analysis_results(stories, output_dir)
    
    # Lưu thông tin báo cáo
    report = AnalysisReport(
        title=f"Phân tích truyện - {timestamp}",
        description=f"Phân tích {len(stories)} truyện",
        source=",".join(set(Website.query.get(s.website_id).api_name for s in stories if s.website_id)),
        story_count=len(stories),
        comment_count=sum(len(s.comments) for s in stories),
        excel_path=excel_path
    )
    
    db.session.add(report)
    db.session.commit()
    
    return send_file(excel_path, as_attachment=True)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    socketio.run(app, debug=app.config['DEBUG'], host='0.0.0.0', port=5000)