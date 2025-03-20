import pandas as pd
import os
from datetime import datetime

class ExcelExporter:
    """Xuất dữ liệu phân tích ra Excel"""
    
    @staticmethod
    def export_analysis_results(stories, output_dir=None):
        """
        Xuất kết quả phân tích thành file Excel
        
        Args:
            stories: List các đối tượng Story đã phân tích
            output_dir: Thư mục output (None để tạo tự động)
            
        Returns:
            str: Đường dẫn đến file Excel
        """
        # Tạo thư mục output nếu chưa có
        if not output_dir:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = f"output_{timestamp}"
            os.makedirs(output_dir, exist_ok=True)
        
        # Tạo dataframe cho thông tin truyện
        story_data = []
        all_comments = []
        
        for story in stories:
            # Lấy thông tin website
            website_name = story.website.name if story.website else "Unknown"
            
            # Thêm dữ liệu truyện
            story_info = {
                'ID': story.id,
                'Tên truyện': story.title,
                'Tên khác': story.alt_title,
                'Tác giả': story.author,
                'Trạng thái': story.status,
                'Thể loại': story.genres,
                'Website': website_name,
                'Số chương': story.chapter_count,
                'Lượt xem': story.views,
                'Lượt thích': story.likes,
                'Lượt theo dõi': story.follows,
                'Xếp hạng': story.rating,
                'Lượt đánh giá': story.rating_count,
                'Lượt xem/chương': story.views / max(1, story.chapter_count),
                'Lượt thích/chương': story.likes / max(1, story.chapter_count) if story.likes else 0,
                'Lượt theo dõi/chương': story.follows / max(1, story.chapter_count) if story.follows else 0,
                'Số lượng bình luận': len(story.comments),
                'Tỷ lệ bình luận tích cực': story.positive_ratio * 100 if story.positive_ratio else 0,
                'Tỷ lệ bình luận tiêu cực': story.negative_ratio * 100 if story.negative_ratio else 0,
                'Tỷ lệ bình luận trung tính': story.neutral_ratio * 100 if story.neutral_ratio else 0,
                'Điểm lượt xem': story.view_score,
                'Điểm lượt thích': story.like_score,
                'Điểm lượt theo dõi': story.follow_score,
                'Điểm số chương': story.chapter_score,
                'Điểm cơ bản': story.base_rating,
                'Điểm sentiment': story.sentiment_score,
                'Điểm đánh giá tổng hợp': story.final_rating,
                'URL': story.url,
                'Thời gian cập nhật': story.updated_at
            }
            story_data.append(story_info)
            
            # Thêm bình luận
            for comment in story.comments:
                comment_info = {
                    'Tên truyện': story.title,
                    'Người bình luận': comment.username,
                    'Nội dung bình luận': comment.content,
                    'Cảm xúc': comment.sentiment,
                    'Điểm cảm xúc': comment.sentiment_score,
                    'Thời gian bình luận': comment.date
                }
                all_comments.append(comment_info)
        
        # Tạo DataFrames
        story_df = pd.DataFrame(story_data)
        comments_df = pd.DataFrame(all_comments)
        
        # Sắp xếp theo điểm đánh giá tổng hợp
        story_df = story_df.sort_values(by='Điểm đánh giá tổng hợp', ascending=False)
        
        # Thêm cột xếp hạng
        story_df.insert(0, 'Xếp hạng', range(1, len(story_df) + 1))
        
        # Tạo đường dẫn file Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_file = os.path.join(output_dir, f"comic_analysis_{timestamp}.xlsx")
        
        # Xuất ra Excel
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            story_df.to_excel(writer, sheet_name='Đánh giá truyện', index=False)
            comments_df.to_excel(writer, sheet_name='Phân tích bình luận', index=False)
            
            # Tạo sheet thống kê
            stats = {
                'Chỉ số': [
                    'Tổng số truyện', 
                    'Tổng số bình luận', 
                    'Tỷ lệ bình luận tích cực',
                    'Tỷ lệ bình luận tiêu cực',
                    'Tỷ lệ bình luận trung tính',
                    'Truyện có điểm cao nhất',
                    'Điểm cao nhất',
                    'Truyện có nhiều bình luận nhất',
                    'Thời gian phân tích'
                ],
                'Giá trị': [
                    len(stories),
                    len(all_comments),
                    f"{round(sum(s.positive_ratio or 0 for s in stories) / max(1, len(stories)) * 100, 2)}%",
                    f"{round(sum(s.negative_ratio or 0 for s in stories) / max(1, len(stories)) * 100, 2)}%",
                    f"{round(sum(s.neutral_ratio or 0 for s in stories) / max(1, len(stories)) * 100, 2)}%",
                    story_df['Tên truyện'].iloc[0] if not story_df.empty else "N/A",
                    round(story_df['Điểm đánh giá tổng hợp'].iloc[0], 2) if not story_df.empty else 0,
                    max(stories, key=lambda s: len(s.comments)).title if stories else "N/A",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ]
            }
            pd.DataFrame(stats).to_excel(writer, sheet_name='Thống kê', index=False)
        
        return excel_file