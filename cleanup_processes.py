# cleanup_processes.py
import os
import psutil
import sys
import time
import logging
from pathlib import Path

# Thiết lập logging
log_dir = Path(__file__).parent / "logs"
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    filename=log_dir / "cleanup.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def find_and_terminate_processes():
    """Tìm và kết thúc các tiến trình liên quan đến ứng dụng"""
    terminated = 0
    try:
        # Tìm tất cả các tiến trình của Chrome và chromedriver
        chrome_procs = []
        python_procs = []
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                proc_name = proc.info['name'].lower() if proc.info['name'] else ''
                cmdline = proc.info['cmdline'] if proc.info['cmdline'] else []
                cmdline_str = ' '.join(cmdline).lower()
                
                # Tìm chrome và chromedriver
                if 'chrome' in proc_name or 'chromedriver' in proc_name:
                    # Kiểm tra xem có liên quan đến tự động hóa không
                    if any(term in cmdline_str for term in ['--headless', 'webdriver', 'automation']):
                        chrome_procs.append(proc)
                
                # Tìm các process Python có thể liên quan đến crawler
                if 'python' in proc_name:
                    if any(term in cmdline_str for term in ['crawler', 'selenium', 'ratingcomic', 'webdriver']):
                        python_procs.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Kết thúc các tiến trình Chrome trước
        for proc in chrome_procs:
            try:
                proc.terminate()
                logging.info(f"Đã kết thúc tiến trình Chrome: {proc.pid}")
                terminated += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                try:
                    proc.kill()
                    logging.info(f"Đã buộc kết thúc tiến trình Chrome: {proc.pid}")
                    terminated += 1
                except:
                    pass
        
        # Đợi một chút để Chrome đóng
        time.sleep(1)
        
        # Kết thúc các tiến trình Python
        for proc in python_procs:
            # Bỏ qua tiến trình hiện tại
            if proc.pid == os.getpid():
                continue
                
            try:
                proc.terminate()
                logging.info(f"Đã kết thúc tiến trình Python: {proc.pid}")
                terminated += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                try:
                    proc.kill()
                    logging.info(f"Đã buộc kết thúc tiến trình Python: {proc.pid}")
                    terminated += 1
                except:
                    pass
        
        return terminated
    except Exception as e:
        logging.error(f"Lỗi khi dọn dẹp tiến trình: {e}")
        return 0

def cleanup_temp_files():
    """Dọn dẹp các file tạm có thể bị bỏ lại"""
    cleaned = 0
    try:
        # Đường dẫn đến thư mục dự án
        project_dir = Path(__file__).parent
        
        # Các thư mục cần kiểm tra
        dirs_to_check = [
            project_dir / "temp",
            project_dir / "logs" / "temp",
            Path(os.environ.get('TEMP', 'C:/Windows/Temp'))
        ]
        
        # Các pattern file tạm
        temp_patterns = [
            "scoped_dir*",
            "chrome_*",
            "_MEI*",
            "selenium*",
            "tmp*"
        ]
        
        for directory in dirs_to_check:
            if directory.exists():
                for pattern in temp_patterns:
                    for file_path in directory.glob(pattern):
                        try:
                            if file_path.is_file():
                                file_path.unlink()
                                cleaned += 1
                            elif file_path.is_dir():
                                import shutil
                                shutil.rmtree(file_path, ignore_errors=True)
                                cleaned += 1
                        except:
                            pass
        
        return cleaned
    except Exception as e:
        logging.error(f"Lỗi khi dọn dẹp file tạm: {e}")
        return 0

if __name__ == "__main__":
    logging.info("Bắt đầu quá trình dọn dẹp")
    terminated = find_and_terminate_processes()
    cleaned = cleanup_temp_files()
    logging.info(f"Kết thúc quá trình dọn dẹp: {terminated} tiến trình đã kết thúc, {cleaned} file tạm đã dọn dẹp")
    print(f"Đã kết thúc {terminated} tiến trình và dọn dẹp {cleaned} file tạm")