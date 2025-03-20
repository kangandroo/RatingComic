from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, send_from_directory
from flask_socketio import SocketIO, emit
import os
import json
import time
from datetime import datetime
import threading
import eventlet

# Sử dụng eventlet để tăng hiệu suất cho Flask-SocketIO
eventlet.monkey_patch()

# Import các modules
from db.models import db, Website, StoryIndex, AnalysisReport
from database.db_manager import DatabaseManager
from database.operations import DatabaseOperations
from crawlers.nettruyen import NetTruyenCrawler
from crawlers.base_crawler import BaseCrawler  
from analyzers.sentiment import SentimentAnalyzer
from analyzers.rating import ComicRatingCalculator
from utils.logger import default_logger as logger
from utils.logger import crawler_logger, analyzer_logger

# Khởi tạo Flask app
app = Flask(__name__)
app.config.from_object('config.Config')
app.secret_key = app.config['SECRET_KEY']

# Thêm cấu hình cho database riêng
app.config['STORY_DB_DIR'] = os.environ.get('STORY_DB_DIR', 'databases/stories')

# Khởi tạo SocketIO
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Khởi tạo database master
db.init_app(app)

# Khởi tạo database manager
db_manager = DatabaseManager(app)

# Khởi tạo operations
db_ops = DatabaseOperations(app, db_manager)

# Tạo thư mục cần thiết
os.makedirs(app.config.get('UPLOAD_FOLDER', 'uploads'), exist_ok=True)
os.makedirs(app.config.get('EXPORT_FOLDER', 'exports'), exist_ok=True)
os.makedirs(app.config.get('STORY_DB_DIR', 'databases/stories'), exist_ok=True)

# Background task flag
background_tasks = {}

def emit_log(message, room=None):
    """Gửi log qua socket"""
    data = {
        'message': message,
        'timestamp': datetime.now().strftime('%H:%M:%S')
    }
    socketio.emit('log_message', data, room=room)

@app.before_first_request
def create_tables():
    """Tạo bảng và dữ liệu ban đầu"""
    with app.app_context():
        # Tạo tất cả bảng
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
                
            # Kích hoạt ràng buộc khóa ngoại cho SQLite
            db.session.execute("PRAGMA foreign_keys=ON")
                
            db.session.commit()
            logger.info("Đã khởi tạo dữ liệu website")

def get_crawler(source):
    """Lấy crawler tương ứng với nguồn"""
    if source == "nettruyen":
        return NetTruyenCrawler(
            logger=crawler_logger,
            chromedriver_path=app.config.get('CHROME_DRIVER_PATH')
        )
    # Thêm các crawler khác ở đây
    return None

@app.route('/')
def index():
    """Trang chủ"""
    # Lấy thông tin tổng quan
    story_count = StoryIndex.query.count()
    website_count = Website.query.count()
    analyzed_count = StoryIndex.query.filter_by(has_analyzed=True).count()
    reports_count = AnalysisReport.query.count()
    
    # Lấy 5 truyện mới nhất
    latest_stories = StoryIndex.query.order_by(StoryIndex.created_at.desc()).limit(5).all()
    
    # Lấy 5 truyện có điểm cao nhất
    top_stories = StoryIndex.query.filter(StoryIndex.final_rating.isnot(None))\
        .order_by(StoryIndex.final_rating.desc()).limit(5).all()
    
    return render_template(
        'index.html',
        story_count=story_count,
        website_count=website_count,
        analyzed_count=analyzed_count,
        reports_count=reports_count,
        latest_stories=latest_stories,
        top_stories=top_stories
    )

@app.route('/crawl', methods=['GET', 'POST'])
def crawl():
    """Trang crawl dữ liệu"""
    if request.method == 'POST':
        source = request.form.get('source')
        num_pages = int(request.form.get('num_pages', 1))
        
        # Tạo task ID
        task_id = f"crawl_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Khởi tạo background task
        background_tasks[task_id] = {
            'status': 'starting',
            'progress': 0,
            'total': 0,
            'message': 'Đang khởi động...'
        }
        
        # Bắt đầu task
        threading.Thread(
            target=crawl_data_task,
            args=(source, num_pages, task_id)
        ).start()
        
        return redirect(url_for('process_crawl', task_id=task_id))
    
    # GET request
    websites = Website.query.all()
    return render_template('crawl.html', websites=websites)

@app.route('/process_crawl/<task_id>')
def process_crawl(task_id):
    """Trang hiển thị tiến trình crawl"""
    task_info = background_tasks.get(task_id, {})
    if not task_info:
        flash('Task không tồn tại', 'error')
        return redirect(url_for('crawl'))
    
    return render_template('process_crawl.html', task_id=task_id, task_info=task_info)

def crawl_data_task(source, num_pages, task_id):
    """Task crawl dữ liệu chạy trong background"""
    task_info = background_tasks[task_id]
    task_info['status'] = 'running'
    
    try:
        # Lấy website ID
        website = Website.query.filter_by(api_name=source).first()
        if not website:
            task_info['status'] = 'error'
            task_info['message'] = f'Không tìm thấy website với API name: {source}'
            return
        
        # Khởi tạo crawler
        crawler = get_crawler(source)
        if not crawler:
            task_info['status'] = 'error'
            task_info['message'] = f'Không hỗ trợ crawler cho {source}'
            return
        
        # Kết nối Socket.IO cho task này
        def emit_task_log(message):
            data = {
                'message': message,
                'timestamp': datetime.now().strftime('%H:%M:%S')
            }
            socketio.emit('task_log', data, room=task_id)
            task_info['message'] = message
        
        # Gửi thông báo bắt đầu
        emit_task_log(f"Bắt đầu crawl dữ liệu từ {website.name}")
        
        # Lấy danh sách truyện
        start_time = time.time()
        stories = crawler.get_all_stories(num_pages=num_pages, emit_log=emit_task_log)
        
        # Cập nhật task info
        task_info['total'] = len(stories)
        task_info['progress'] = 0
        
        # Lưu truyện vào database
        for i, story_data in enumerate(stories):
            try:
                # Thêm vào chỉ mục
                with app.app_context():
                    story_index = db_ops.add_story_index(story_data, website.id)
                    
                    # Lấy chi tiết và lưu
                    detailed_data = crawler.get_story_details(story_data, emit_log=emit_task_log)
                    db_ops.add_story_detail(story_index.id, detailed_data)
                
                # Cập nhật tiến độ
                task_info['progress'] = i + 1
                progress_pct = int((i + 1) / len(stories) * 100)
                emit_task_log(f"Đã xử lý {i+1}/{len(stories)} truyện ({progress_pct}%)")
                
            except Exception as e:
                emit_task_log(f"Lỗi khi xử lý truyện {story_data.get('title', '')}: {str(e)}")
        
        # Kết thúc
        elapsed_time = time.time() - start_time
        emit_task_log(f"Đã hoàn thành crawl {len(stories)} truyện trong {elapsed_time:.2f} giây")
        task_info['status'] = 'completed'
        
    except Exception as e:
        task_info['status'] = 'error'
        task_info['message'] = f'Lỗi: {str(e)}'
        logger.exception(f"Lỗi khi crawl dữ liệu: {str(e)}")

@app.route('/stories')
def stories_list():
    """Danh sách truyện"""
    # Lấy tham số lọc
    website_id = request.args.get('website_id', type=int)
    min_views = request.args.get('min_views', type=int)
    has_analyzed = request.args.get('has_analyzed', default=None)
    if has_analyzed is not None:
        has_analyzed = has_analyzed == '1'
    
    # Lấy danh sách truyện
    stories = db_ops.get_stories(
        website_id=website_id,
        min_views=min_views,
        has_analyzed=has_analyzed
    )
    
    # Lấy danh sách website
    websites = Website.query.all()
    
    return render_template(
        'stories_list.html',
        stories=stories,
        websites=websites,
        filters={
            'website_id': website_id,
            'min_views': min_views,
            'has_analyzed': has_analyzed
        }
    )

@app.route('/analyze', methods=['POST'])
def analyze():
    """Phân tích truyện"""
    story_ids = request.form.getlist('story_ids')
    
    if not story_ids:
        flash('Vui lòng chọn ít nhất một truyện để phân tích', 'error')
        return redirect(url_for('stories_list'))
    
    # Tạo task ID
    task_id = f"analyze_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Khởi tạo background task
    background_tasks[task_id] = {
        'status': 'starting',
        'progress': 0,
        'total': len(story_ids),
        'message': 'Đang khởi động...'
    }
    
    # Bắt đầu task
    threading.Thread(
        target=analyze_stories_task,
        args=(story_ids, task_id)
    ).start()
    
    return redirect(url_for('process_analyze', task_id=task_id))

@app.route('/process_analyze/<task_id>')
def process_analyze(task_id):
    """Trang hiển thị tiến trình phân tích"""
    task_info = background_tasks.get(task_id, {})
    if not task_info:
        flash('Task không tồn tại', 'error')
        return redirect(url_for('stories_list'))
    
    return render_template('process_analyze.html', task_id=task_id, task_info=task_info)

def analyze_stories_task(story_ids, task_id):
    """Task phân tích truyện chạy trong background"""
    task_info = background_tasks[task_id]
    task_info['status'] = 'running'
    
    try:
        # Khởi tạo analyzer
        sentiment_analyzer = SentimentAnalyzer(
            model_name=app.config.get('SENTIMENT_MODEL', 'xlm-roberta-base'),
            cache_dir=app.config.get('SENTIMENT_CACHE_DIR', '.cache/huggingface'),
            logger=analyzer_logger
        )
        
        rating_calculator = ComicRatingCalculator(logger=analyzer_logger)
        
        # Kết nối Socket.IO cho task này
        def emit_task_log(message):
            data = {
                'message': message,
                'timestamp': datetime.now().strftime('%H:%M:%S')
            }
            socketio.emit('task_log', data, room=task_id)
            task_info['message'] = message
        
        # Tải model sentiment
        emit_task_log("Đang tải model phân tích cảm xúc...")
        sentiment_analyzer.load_model(emit_log=emit_task_log)
        
        # Gửi thông báo bắt đầu
        emit_task_log(f"Bắt đầu phân tích {len(story_ids)} truyện")
        
        # Phân tích từng truyện
        start_time = time.time()
        analyzed_stories = []
        total_comments = 0
        
        for i, story_id in enumerate(story_ids):
            try:
                # Lấy thông tin truyện
                with app.app_context():
                    # Lấy thông tin cơ bản
                    story_index = StoryIndex.query.get(story_id)
                    if not story_index:
                        emit_task_log(f"Không tìm thấy truyện ID {story_id}")
                        continue
                    
                    # Lấy thông tin chi tiết
                    story_detail = db_ops.get_story_details(story_id)
                    if not story_detail:
                        emit_task_log(f"Không tìm thấy chi tiết truyện ID {story_id}")
                        continue
                    
                    emit_task_log(f"Đang phân tích truyện: {story_detail['title']}")
                    
                    # Lấy crawler tương ứng
                    website = Website.query.get(story_index.website_id)
                    crawler = get_crawler(website.api_name) if website else None
                    
                    # Nếu chưa có bình luận, crawl bình luận
                    if 'comments' not in story_detail or not story_detail['comments']:
                        if crawler:
                            emit_task_log(f"Đang crawl bình luận cho truyện: {story_detail['title']}")
                            comments_data = crawler.get_comments(story_detail, emit_log=emit_task_log)
                            db_ops.add_comments(story_id, comments_data)
                            
                            # Refresh story_detail
                            story_detail = db_ops.get_story_details(story_id)
                    
                    # Phân tích sentiment các bình luận
                    if 'comments' in story_detail and story_detail['comments']:
                        emit_task_log(f"Đang phân tích {len(story_detail['comments'])} bình luận")
                        
                        # Phân tích tất cả bình luận
                        analyzed_comments, sentiment_stats = sentiment_analyzer.analyze_comments(
                            story_detail['comments'],
                            emit_log=emit_task_log
                        )
                        
                        # Cập nhật sentiment cho từng bình luận
                        for comment in analyzed_comments:
                            db_ops.update_comment_sentiment(
                                story_id,
                                comment['id'],
                                comment['sentiment'],
                                comment['sentiment_score']
                            )
                        
                        total_comments += len(analyzed_comments)
                    else:
                        emit_task_log(f"Không có bình luận cho truyện: {story_detail['title']}")
                        sentiment_stats = {
                            "positive_ratio": 0,
                            "negative_ratio": 0,
                            "neutral_ratio": 1,
                            "sentiment_score": 5.0
                        }
                    
                    # Tính điểm đánh giá
                    emit_task_log(f"Đang tính điểm đánh giá cho truyện: {story_detail['title']}")
                    ratings = rating_calculator.calculate_full_rating(story_detail, sentiment_stats)
                    
                    # Lưu kết quả đánh giá
                    db_ops.update_story_ratings(story_id, ratings)
                    
                    # Thêm vào danh sách đã phân tích
                    analyzed_stories.append(story_id)
                
                # Cập nhật tiến độ
                task_info['progress'] = i + 1
                progress_pct = int((i + 1) / len(story_ids) * 100)
                emit_task_log(f"Đã phân tích {i+1}/{len(story_ids)} truyện ({progress_pct}%)")
                
            except Exception as e:
                emit_task_log(f"Lỗi khi phân tích truyện ID {story_id}: {str(e)}")
        
        # Xuất kết quả ra Excel
        if analyzed_stories:
            emit_task_log(f"Đang xuất kết quả phân tích ra Excel...")
            excel_file = db_ops.export_to_excel(analyzed_stories, output_dir=app.config.get('EXPORT_FOLDER', 'exports'))
            
            # Lưu báo cáo phân tích
            if excel_file:
                report_title = f"Phân tích truyện {datetime.now().strftime('%Y-%m-%d %H:%M')}"
                db_ops.add_analysis_report(
                    title=report_title,
                    description=f"Phân tích {len(analyzed_stories)} truyện với tổng cộng {total_comments} bình luận",
                    story_ids=analyzed_stories,
                    source="Web Interface",
                    story_count=len(analyzed_stories),
                    comment_count=total_comments,
                    excel_path=excel_file
                )
                
                emit_task_log(f"Đã xuất kết quả phân tích vào file: {excel_file}")
            else:
                emit_task_log("Không thể xuất kết quả phân tích ra Excel")
        
        # Kết thúc
        elapsed_time = time.time() - start_time
        emit_task_log(f"Đã hoàn thành phân tích {len(analyzed_stories)}/{len(story_ids)} truyện trong {elapsed_time:.2f} giây")
        task_info['status'] = 'completed'
        
    except Exception as e:
        task_info['status'] = 'error'
        task_info['message'] = f'Lỗi: {str(e)}'
        logger.exception(f"Lỗi khi phân tích truyện: {str(e)}")

@app.route('/story/<int:story_id>')
def story_detail(story_id):
    """Chi tiết truyện"""
    # Lấy thông tin truyện
    story_detail = db_ops.get_story_details(story_id)
    if not story_detail:
        flash('Không tìm thấy truyện', 'error')
        return redirect(url_for('stories_list'))
    
    return render_template('story_detail.html', story=story_detail)

@app.route('/reports')
def reports_list():
    """Danh sách báo cáo phân tích"""
    reports = AnalysisReport.query.order_by(AnalysisReport.created_at.desc()).all()
    return render_template('reports.html', reports=reports)

@app.route('/export/<path:filename>')
def download_export(filename):
    """Tải file xuất"""
    export_folder = app.config.get('EXPORT_FOLDER', 'exports')
    return send_from_directory(export_folder, filename)

@socketio.on('connect')
def handle_connect():
    """Xử lý kết nối Socket.IO"""
    logger.info(f"Client connected: {request.sid}")

@socketio.on('join_task')
def on_join_task(data):
    """Tham gia room của task"""
    task_id = data.get('task_id')
    if task_id:
        from flask_socketio import join_room
        join_room(task_id)
        emit('joined_task', {'task_id': task_id})
        
        # Gửi trạng thái hiện tại của task
        task_info = background_tasks.get(task_id, {})
        emit('task_status', task_info)

@socketio.on('disconnect')
def handle_disconnect():
    """Xử lý ngắt kết nối Socket.IO"""
    logger.info(f"Client disconnected: {request.sid}")

@app.route('/api/task_status/<task_id>')
def get_task_status(task_id):
    """API lấy trạng thái task"""
    task_info = background_tasks.get(task_id, {})
    return jsonify(task_info)

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        # Tối ưu SQLite
        from database.operations import DatabaseOperations
        db_manager.optimize_sqlite_db('databases/master.db')
    socketio.run(app, debug=app.config['DEBUG'], host='0.0.0.0', port=5000)