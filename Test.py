#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AMD Crash Diagnostic Tool
Công cụ này giúp chẩn đoán nguyên nhân gây crash trên hệ thống AMD
mà không xảy ra trên hệ thống Intel.
"""

import os
import sys
import platform
import subprocess
import tempfile
import traceback
import logging
import gc
import threading
import multiprocessing
import ctypes
import time
import signal
import psutil
from datetime import datetime
import json
import socket
from contextlib import contextmanager

# Thiết lập logging cơ bản
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("amd_diagnostics.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("amd_diagnostics")

# Phát hiện nền tảng
IS_WINDOWS = platform.system() == "Windows"
IS_LINUX = platform.system() == "Linux"
IS_MAC = platform.system() == "Darwin"

class TimeoutError(Exception):
    pass

@contextmanager
def time_limit(seconds):
    """Giới hạn thời gian thực thi của một block code"""
    def signal_handler(signum, frame):
        raise TimeoutError(f"Thời gian thực thi vượt quá {seconds} giây")
    
    if not IS_WINDOWS:  # signal.SIGALRM không hoạt động trên Windows
        signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
    
    try:
        yield
    finally:
        if not IS_WINDOWS:
            signal.alarm(0)  # Tắt báo thức

class CPUInfo:
    """Thu thập và phân tích thông tin CPU"""
    
    @staticmethod
    def get_cpu_info():
        """Thu thập thông tin CPU chi tiết"""
        info = {
            "processor": platform.processor(),
            "machine": platform.machine(),
            "architecture": platform.architecture(),
            "system": platform.system(),
            "python_compiler": platform.python_compiler(),
            "is_amd": "amd" in platform.processor().lower()
        }
        
        # Thêm thông tin chi tiết về CPU
        if IS_LINUX:
            try:
                with open("/proc/cpuinfo", "r") as f:
                    cpuinfo = f.read()
                
                for line in cpuinfo.split("\n"):
                    if "model name" in line:
                        info["model_name"] = line.split(":")[1].strip()
                        break
            except Exception as e:
                logger.error(f"Không thể đọc /proc/cpuinfo: {e}")
        
        elif IS_WINDOWS:
            try:
                import wmi
                w = wmi.WMI()
                processor = w.Win32_Processor()[0]
                info["model_name"] = processor.Name
                info["manufacturer"] = processor.Manufacturer
                info["cores"] = processor.NumberOfCores
                info["logical_processors"] = processor.NumberOfLogicalProcessors
            except ImportError:
                try:
                    # Thử sử dụng subprocess nếu wmi không có sẵn
                    result = subprocess.check_output("wmic cpu get name", shell=True).decode().strip()
                    lines = result.split('\n')
                    if len(lines) > 1:
                        info["model_name"] = lines[1].strip()
                except Exception as e:
                    logger.error(f"Không thể lấy thông tin CPU từ WMI: {e}")
        
        # Kiểm tra các tập lệnh CPU nâng cao
        info["extensions"] = CPUInfo.get_cpu_extensions()
        
        return info
    
    @staticmethod
    def get_cpu_extensions():
        """Kiểm tra bộ lệnh mở rộng của CPU như SSE, AVX, etc."""
        extensions = {}
        
        try:
            import numpy as np
            np_config = np.show_config()
            extensions["numpy_config"] = str(np_config)
        except ImportError:
            logger.warning("NumPy không được cài đặt, không thể kiểm tra cấu hình NumPy")
        
        try:
            # Thử tải cpuinfo nếu có
            import cpuinfo
            info = cpuinfo.get_cpu_info()
            if 'flags' in info:
                extensions["flags"] = info['flags']
                
                # Kiểm tra các bộ lệnh quan trọng
                for ext in ['sse', 'sse2', 'sse3', 'ssse3', 'sse4_1', 'sse4_2', 'avx', 'avx2', 'fma', 'avx512f']:
                    extensions[ext] = ext in info['flags']
            
        except ImportError:
            logger.warning("Module py-cpuinfo không được cài đặt, không thể kiểm tra flags CPU")
            
            # Thử đọc thông tin SSE/AVX từ thiết bị
            if IS_LINUX:
                try:
                    with open("/proc/cpuinfo", "r") as f:
                        cpuinfo = f.read()
                    
                    # Tìm flags
                    for line in cpuinfo.split("\n"):
                        if "flags" in line or "Features" in line:
                            flags = line.split(":")[1].strip().split()
                            extensions["flags"] = flags
                            
                            # Kiểm tra các bộ lệnh quan trọng
                            for ext in ['sse', 'sse2', 'sse3', 'ssse3', 'sse4_1', 'sse4_2', 'avx', 'avx2', 'fma', 'avx512f']:
                                extensions[ext] = ext in flags
                            break
                except Exception as e:
                    logger.error(f"Không thể đọc CPU flags từ /proc/cpuinfo: {e}")
            elif IS_WINDOWS:
                # Windows cần công cụ bên ngoài để kiểm tra CPU features
                logger.warning("Không thể kiểm tra chi tiết CPU flags trên Windows mà không có thư viện phụ trợ")
        
        return extensions

class MemoryTest:
    """Các bài kiểm tra liên quan đến bộ nhớ"""
    
    @staticmethod
    def monitor_memory_usage(duration=10, interval=0.5):
        """Giám sát sử dụng bộ nhớ trong một khoảng thời gian"""
        logger.info(f"Giám sát sử dụng bộ nhớ trong {duration} giây...")
        
        process = psutil.Process(os.getpid())
        start_time = time.time()
        usage_data = []
        
        while time.time() - start_time < duration:
            mem_info = process.memory_info()
            usage_data.append({
                'time': time.time() - start_time,
                'rss': mem_info.rss / 1024 / 1024,  # MB
                'vms': mem_info.vms / 1024 / 1024   # MB
            })
            time.sleep(interval)
        
        # Phân tích dữ liệu
        if usage_data:
            max_rss = max(data['rss'] for data in usage_data)
            min_rss = min(data['rss'] for data in usage_data)
            avg_rss = sum(data['rss'] for data in usage_data) / len(usage_data)
            
            logger.info(f"Sử dụng bộ nhớ (RSS): Min={min_rss:.2f}MB, Max={max_rss:.2f}MB, Avg={avg_rss:.2f}MB")
            
            # Kiểm tra rò rỉ bộ nhớ 
            leak_threshold = (max_rss - min_rss) / max_rss
            if leak_threshold > 0.2:  # Nếu tăng hơn 20%
                logger.warning(f"Có dấu hiệu rò rỉ bộ nhớ! Tăng {leak_threshold*100:.2f}% trong thời gian giám sát")

        return usage_data
    
    @staticmethod
    def test_memory_allocation():
        """Kiểm tra khả năng phân bổ bộ nhớ lớn"""
        logger.info("Kiểm tra phân bổ bộ nhớ...")
        
        # Kích thước của mảng sẽ tăng dần
        sizes = [10, 50, 100, 200, 500]  # MB
        results = []
        
        for size_mb in sizes:
            try:
                size_bytes = size_mb * 1024 * 1024
                elements = size_bytes // 8  # Số phần tử double precision (8 bytes)
                
                logger.info(f"Thử phân bổ mảng {size_mb}MB ({elements:,} phần tử)...")
                
                # Ghi lại bộ nhớ trước khi phân bổ
                before_mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                
                # Thử phân bổ
                start_time = time.time()
                large_array = [0.0] * elements
                allocation_time = time.time() - start_time
                
                # Thử sử dụng mảng
                start_time = time.time()
                for i in range(0, elements, elements // 100):  # Chỉ truy cập 1% các phần tử
                    large_array[i] = i
                access_time = time.time() - start_time
                
                # Ghi lại bộ nhớ sau khi phân bổ
                after_mem = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                
                logger.info(f"Phân bổ {size_mb}MB thành công trong {allocation_time:.4f}s, truy cập trong {access_time:.4f}s")
                logger.info(f"Bộ nhớ thực tế tăng: {after_mem-before_mem:.2f}MB")
                
                results.append({
                    'size_mb': size_mb,
                    'success': True,
                    'allocation_time': allocation_time,
                    'access_time': access_time,
                    'memory_increase': after_mem - before_mem
                })
                
                # Giải phóng bộ nhớ
                del large_array
                gc.collect()
                time.sleep(0.5)  # Cho thời gian để giải phóng
                
            except MemoryError as e:
                logger.error(f"Memory Error khi phân bổ {size_mb}MB: {e}")
                results.append({
                    'size_mb': size_mb,
                    'success': False,
                    'error': str(e)
                })
                break
                
            except Exception as e:
                logger.error(f"Lỗi không mong đợi khi phân bổ {size_mb}MB: {e}")
                logger.error(traceback.format_exc())
                results.append({
                    'size_mb': size_mb,
                    'success': False,
                    'error': str(e)
                })
                break
        
        return results
    
    @staticmethod
    def test_memory_fragmentation():
        """Kiểm tra phân mảnh bộ nhớ"""
        logger.info("Kiểm tra phân mảnh bộ nhớ...")
        
        try:
            # Tạo và xóa nhiều đối tượng có kích thước khác nhau
            for i in range(10):
                objects = []
                for j in range(10000):
                    # Tạo đối tượng ngẫu nhiên với kích cỡ khác nhau
                    if j % 3 == 0:
                        objects.append("X" * (j % 100 + 1))
                    elif j % 3 == 1:
                        objects.append([0] * (j % 50 + 1))
                    else:
                        d = {}
                        for k in range(j % 20 + 1):
                            d[str(k)] = "value" + str(k)
                        objects.append(d)
                
                # Ghi lại sử dụng bộ nhớ
                mem_usage = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                logger.info(f"Vòng lặp {i+1}: Sử dụng {mem_usage:.2f}MB sau khi tạo {len(objects)} đối tượng")
                
                # Xóa và thu gom rác
                del objects
                gc.collect()
                
                # Ghi lại sử dụng bộ nhớ sau khi dọn dẹp
                after_gc = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                logger.info(f"Vòng lặp {i+1}: Sử dụng {after_gc:.2f}MB sau khi giải phóng bộ nhớ")
                
                # Nếu bộ nhớ tăng quá nhiều sau mỗi chu kỳ, có thể có vấn đề phân mảnh
                if i > 0 and after_gc > 1.5 * initial_mem:
                    logger.warning(f"Dấu hiệu phân mảnh bộ nhớ: {after_gc/initial_mem:.2f}x sau {i+1} vòng lặp")
                
                # Lưu mức sử dụng ban đầu
                if i == 0:
                    initial_mem = after_gc
            
            return {'initial_memory': initial_mem, 'final_memory': after_gc, 'ratio': after_gc/initial_mem}
            
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra phân mảnh bộ nhớ: {e}")
            logger.error(traceback.format_exc())
            return {'error': str(e)}

class ConcurrencyTest:
    """Các bài kiểm tra liên quan đến xử lý đồng thời"""
    
    @staticmethod
    def test_threading(num_threads=10, iterations=100000):
        """Kiểm tra xử lý đa luồng"""
        logger.info(f"Kiểm tra đa luồng với {num_threads} threads...")
        
        # Biến chia sẻ và lock
        counter = [0]
        lock = threading.Lock()
        errors = [0]
        start_time = time.time()
        
        def worker():
            try:
                for _ in range(iterations):
                    with lock:
                        counter[0] += 1
            except Exception as e:
                logger.error(f"Lỗi trong thread: {e}")
                errors[0] += 1
        
        # Tạo và chạy các thread
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=worker)
            threads.append(t)
            t.start()
        
        # Đợi các thread hoàn thành
        for t in threads:
            t.join()
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        expected_value = num_threads * iterations
        actual_value = counter[0]
        
        if expected_value != actual_value:
            logger.error(f"Lỗi đồng bộ hóa thread! Mong đợi {expected_value}, nhận được {actual_value}")
        else:
            logger.info(f"Kiểm tra đa luồng thành công: {num_threads} threads × {iterations} lần lặp trong {execution_time:.2f}s")
        
        return {
            'threads': num_threads,
            'iterations': iterations,
            'expected': expected_value,
            'actual': actual_value,
            'execution_time': execution_time,
            'errors': errors[0]
        }
    
    @staticmethod
    def test_multiprocessing(num_processes=4):
        """Kiểm tra xử lý đa tiến trình"""
        logger.info(f"Kiểm tra đa tiến trình với {num_processes} tiến trình...")
        
        start_time = time.time()
        
        def cpu_intensive_task(x):
            """Tác vụ tính toán nặng tại mỗi tiến trình"""
            try:
                # Tính Fibonacci bằng phương pháp lặp
                result = 0
                a, b = 0, 1
                for i in range(1000000):  # Đủ dài để tạo tải CPU
                    a, b = b, a + b
                    result = b
                return {'process_id': x, 'success': True, 'result': result}
            except Exception as e:
                return {'process_id': x, 'success': False, 'error': str(e)}
        
        try:
            # Sử dụng pool processes
            with multiprocessing.Pool(num_processes) as pool:
                results = pool.map(cpu_intensive_task, range(num_processes))
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Kiểm tra kết quả
            success_count = sum(1 for r in results if r['success'])
            logger.info(f"Đa tiến trình hoàn thành: {success_count}/{num_processes} thành công trong {execution_time:.2f}s")
            
            if success_count < num_processes:
                logger.error(f"Có {num_processes - success_count} tiến trình gặp lỗi!")
                for r in results:
                    if not r['success']:
                        logger.error(f"Tiến trình {r['process_id']} lỗi: {r['error']}")
            
            return {
                'processes': num_processes,
                'successful': success_count,
                'execution_time': execution_time,
                'details': results
            }
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình chạy đa tiến trình: {e}")
            logger.error(traceback.format_exc())
            return {
                'processes': num_processes,
                'error': str(e)
            }
    
    @staticmethod
    def test_race_conditions():
        """Kiểm tra điều kiện đua (race conditions)"""
        logger.info("Kiểm tra điều kiện đua...")
        
        # Thử nghiệm điều kiện đua
        shared_list = []
        iterations = 10000
        num_threads = 5
        errors_detected = [0]
        
        def append_worker():
            for i in range(iterations):
                # Cố ý không dùng lock để kích hoạt điều kiện đua
                shared_list.append(i)
        
        def remove_worker():
            for i in range(iterations):
                try:
                    if shared_list:
                        shared_list.pop()
                except IndexError:
                    errors_detected[0] += 1
        
        threads = []
        # Tạo threads để thêm phần tử
        for i in range(num_threads):
            t = threading.Thread(target=append_worker)
            threads.append(t)
            
        # Tạo threads để xóa phần tử
        for i in range(num_threads):
            t = threading.Thread(target=remove_worker)
            threads.append(t)
        
        # Chạy tất cả threads
        start_time = time.time()
        for t in threads:
            t.start()
            
        # Đợi hoàn thành
        for t in threads:
            t.join()
            
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Phân tích kết quả
        logger.info(f"Kiểm tra điều kiện đua hoàn tất trong {execution_time:.2f}s")
        logger.info(f"Số lỗi IndexError phát hiện: {errors_detected[0]}")
        logger.info(f"Độ dài cuối cùng của danh sách: {len(shared_list)}")
        
        # Nếu độ dài danh sách là 0 và không có lỗi, có thể đã không phát hiện điều kiện đua
        if len(shared_list) == 0 and errors_detected[0] == 0:
            logger.warning("Không phát hiện điều kiện đua - có thể cần kiểm tra kỹ hơn")
        
        return {
            'threads': num_threads * 2,
            'iterations': iterations,
            'execution_time': execution_time,
            'final_list_length': len(shared_list),
            'errors_detected': errors_detected[0]
        }

class CPUInstructionTest:
    """Các bài kiểm tra liên quan đến tập lệnh CPU"""
    
    @staticmethod
    def test_avx():
        """Kiểm tra các tập lệnh AVX"""
        logger.info("Kiểm tra các tập lệnh AVX...")
        
        try:
            # Thử tải NumPy nếu có sẵn
            import numpy as np
            
            # Tạo mảng lớn để thực hiện phép tính Vector
            size = 10000000
            
            # Tính toán với các mảng lớn để sử dụng AVX nếu có
            start_time = time.time()
            a = np.random.random(size)
            b = np.random.random(size)
            c = a * b + a
            execution_time = time.time() - start_time
            
            logger.info(f"Thao tác NumPy vector ({size:,} phần tử) hoàn thành trong {execution_time:.4f}s")
            
            # Thử thao tác ma trận để sử dụng nhiều AVX hơn
            matrix_size = 1000
            start_time = time.time()
            m1 = np.random.random((matrix_size, matrix_size))
            m2 = np.random.random((matrix_size, matrix_size))
            m3 = np.matmul(m1, m2)
            matrix_time = time.time() - start_time
            
            logger.info(f"Nhân ma trận ({matrix_size}x{matrix_size}) hoàn thành trong {matrix_time:.4f}s")
            
            return {
                'vector_time': execution_time,
                'matrix_time': matrix_time,
                'vector_size': size,
                'matrix_size': matrix_size,
                'success': True
            }
            
        except ImportError:
            logger.warning("NumPy không được cài đặt - không thể kiểm tra AVX trực tiếp")
            
            # Cố gắng kiểm tra theo cách khác nếu NumPy không có sẵn
            def manual_vector_calc():
                size = 1000000
                a = [1.0] * size
                b = [2.0] * size
                c = [a[i] * b[i] + a[i] for i in range(size)]
                return c
            
            start_time = time.time()
            manual_vector_calc()
            execution_time = time.time() - start_time
            
            logger.info(f"Tính toán vector thủ công hoàn thành trong {execution_time:.4f}s")
            
            return {
                'manual_vector_time': execution_time,
                'vector_size': 1000000,
                'success': True,
                'numpy_available': False
            }
            
        except Exception as e:
            logger.error(f"Lỗi khi thực hiện kiểm tra AVX: {e}")
            logger.error(traceback.format_exc())
            
            return {
                'success': False,
                'error': str(e)
            }
    
    @staticmethod
    def test_sse():
        """Kiểm tra các tập lệnh SSE"""
        logger.info("Kiểm tra các tập lệnh SSE...")
        
        try:
            # Kiểm tra bằng cách thực hiện phép tính dấu phẩy động nhanh
            def float_intensive():
                result = 0.0
                for i in range(10000000):
                    result += i * 0.1 / (i + 1)
                return result
            
            start_time = time.time()
            result = float_intensive()
            execution_time = time.time() - start_time
            
            logger.info(f"Tính toán số thực chính xác đơn hoàn thành trong {execution_time:.4f}s")
            
            return {
                'execution_time': execution_time,
                'result': result,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Lỗi khi thực hiện kiểm tra SSE: {e}")
            logger.error(traceback.format_exc())
            
            return {
                'success': False,
                'error': str(e)
            }

class NetworkTest:
    """Các bài kiểm tra liên quan đến mạng"""
    
    @staticmethod
    def test_simple_networking():
        """Kiểm tra khả năng kết nối mạng cơ bản"""
        logger.info("Kiểm tra kết nối mạng cơ bản...")
        
        # Một số trang web để kiểm tra
        urls = [
            "https://www.google.com",
            "https://www.github.com",
            "https://www.python.org"
        ]
        
        results = []
        
        import urllib.request
        import urllib.error
        import ssl
        
        # Tạo SSL context không xác minh chứng chỉ
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        for url in urls:
            try:
                logger.info(f"Kết nối tới {url}...")
                start_time = time.time()
                with urllib.request.urlopen(url, context=ctx, timeout=10) as response:
                    data = response.read(10000)  # Đọc 10KB đầu tiên
                execution_time = time.time() - start_time
                
                logger.info(f"Kết nối tới {url} thành công trong {execution_time:.4f}s, nhận {len(data)} bytes")
                results.append({
                    'url': url,
                    'success': True,
                    'time': execution_time,
                    'size': len(data)
                })
                
            except urllib.error.URLError as e:
                logger.error(f"Lỗi kết nối tới {url}: {e}")
                results.append({
                    'url': url,
                    'success': False,
                    'error': str(e)
                })
                
            except Exception as e:
                logger.error(f"Lỗi không mong đợi khi kết nối tới {url}: {e}")
                logger.error(traceback.format_exc())
                results.append({
                    'url': url,
                    'success': False,
                    'error': str(e)
                })
                
        return results
    
    @staticmethod
    def test_concurrent_connections(num_connections=5):
        """Kiểm tra nhiều kết nối đồng thời"""
        logger.info(f"Kiểm tra {num_connections} kết nối đồng thời...")
        
        # URLs để thử nghiệm
        urls = ["https://www.google.com", "https://www.github.com", "https://www.python.org"]
        
        # Chọn URLs với truy cập lặp đi lặp lại
        test_urls = []
        for i in range(num_connections):
            test_urls.append(urls[i % len(urls)])
        
        results = []
        threads = []
        
        def fetch_url(url, index):
            try:
                import urllib.request
                import urllib.error
                import ssl
                
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                
                start_time = time.time()
                with urllib.request.urlopen(url, context=ctx, timeout=15) as response:
                    data = response.read(50000)  # Đọc 50KB
                execution_time = time.time() - start_time
                
                return {
                    'index': index,
                    'url': url,
                    'success': True,
                    'time': execution_time,
                    'size': len(data)
                }
                
            except Exception as e:
                return {
                    'index': index,
                    'url': url,
                    'success': False,
                    'error': str(e)
                }
        
        # Tạo và chạy thread cho mỗi kết nối
        for i, url in enumerate(test_urls):
            thread = threading.Thread(target=lambda u=url, idx=i: results.append(fetch_url(u, idx)))
            threads.append(thread)
            thread.start()
        
        # Đợi tất cả threads hoàn thành
        for thread in threads:
            thread.join(timeout=20)
        
        # Phân tích kết quả
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"{success_count}/{len(test_urls)} kết nối thành công")
        
        # Sắp xếp kết quả theo index
        results.sort(key=lambda x: x['index'])
        
        return results

class FileIOTest:
    """Các bài kiểm tra liên quan đến nhập/xuất file"""
    
    @staticmethod
    def test_file_operations():
        """Kiểm tra các thao tác file cơ bản"""
        logger.info("Kiểm tra thao tác file cơ bản...")
        
        # Tạo đường dẫn file test
        test_dir = tempfile.mkdtemp(prefix="amd_diagnostics_")
        test_file = os.path.join(test_dir, "test_file.txt")
        
        results = {}
        
        try:
            # Thử ghi file
            start_time = time.time()
            with open(test_file, "w") as f:
                for i in range(100000):
                    f.write(f"Line {i}: This is a test line with some content.\n")
            write_time = time.time() - start_time
            
            # Thử đọc file
            start_time = time.time()
            with open(test_file, "r") as f:
                content = f.read()
            read_time = time.time() - start_time
            
            file_size = os.path.getsize(test_file) / (1024 * 1024)  # MB
            
            logger.info(f"Ghi file {file_size:.2f}MB trong {write_time:.4f}s")
            logger.info(f"Đọc file {file_size:.2f}MB trong {read_time:.4f}s")
            
            results = {
                'write_time': write_time,
                'read_time': read_time,
                'file_size_mb': file_size,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Lỗi khi làm việc với file: {e}")
            logger.error(traceback.format_exc())
            results = {
                'success': False,
                'error': str(e)
            }
            
        finally:
            # Dọn dẹp
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
                os.rmdir(test_dir)
            except:
                logger.warning(f"Không thể dọn dẹp file tạm {test_file}")
        
        return results
    
    @staticmethod
    def test_concurrent_file_access():
        """Kiểm tra truy cập file đồng thời từ nhiều thread"""
        logger.info("Kiểm tra truy cập file đồng thời...")
        
        # Tạo đường dẫn file test
        test_dir = tempfile.mkdtemp(prefix="amd_diagnostics_")
        test_file = os.path.join(test_dir, "concurrent_test.txt")
        
        num_threads = 10
        iterations = 1000
        results = {}
        
        try:
            # Tạo file ban đầu
            with open(test_file, "w") as f:
                f.write("0\n" * iterations)
            
            lock = threading.Lock()
            errors = []
            
            def read_write_worker(thread_id):
                try:
                    for i in range(iterations):
                        # Đọc và ghi file, sử dụng lock để tránh race condition
                        with lock:
                            with open(test_file, "r") as f:
                                lines = f.readlines()
                                
                            # Cập nhật dòng tương ứng với thread_id
                            if thread_id < len(lines):
                                lines[thread_id] = f"{int(lines[thread_id].strip()) + 1}\n"
                                
                            with open(test_file, "w") as f:
                                f.writelines(lines)
                except Exception as e:
                    errors.append(f"Thread {thread_id}: {str(e)}")
            
            # Tạo và chạy threads
            start_time = time.time()
            threads = []
            for i in range(num_threads):
                thread = threading.Thread(target=read_write_worker, args=(i,))
                threads.append(thread)
                thread.start()
            
            # Đợi các thread hoàn thành
            for thread in threads:
                thread.join()
                
            execution_time = time.time() - start_time
            
            # Kiểm tra kết quả
            with open(test_file, "r") as f:
                final_lines = f.readlines()
            
            expected_iterations = iterations
            for i in range(num_threads):
                if i < len(final_lines):
                    actual_iterations = int(final_lines[i].strip())
                    if actual_iterations != expected_iterations:
                        logger.warning(f"Thread {i}: Mong đợi {expected_iterations} lần lặp, nhưng thấy {actual_iterations}")
            
            logger.info(f"Truy cập file đồng thời hoàn tất trong {execution_time:.4f}s")
            
            results = {
                'threads': num_threads,
                'iterations': iterations,
                'execution_time': execution_time,
                'errors': errors,
                'success': len(errors) == 0
            }
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình truy cập file đồng thời: {e}")
            logger.error(traceback.format_exc())
            results = {
                'success': False,
                'error': str(e)
            }
            
        finally:
            # Dọn dẹp
            try:
                if os.path.exists(test_file):
                    os.remove(test_file)
                os.rmdir(test_dir)
            except:
                logger.warning(f"Không thể dọn dẹp file tạm {test_file}")
        
        return results

class SimulateCrawler:
    """Mô phỏng crawler đơn giản"""
    
    @staticmethod
    def run_simple_crawler(num_pages=10, concurrency=3):
        """Chạy một crawler đơn giản để mô phỏng khả năng crawl thực tế"""
        logger.info(f"Chạy crawler mô phỏng với {num_pages} trang, độ đồng thời {concurrency}...")
        
        import urllib.request
        import urllib.error
        import ssl
        import random
        import queue
        
        # Tạo SSL context không xác minh chứng chỉ
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        # Danh sách URL để crawl
        urls = [
            "https://www.python.org",
            "https://www.github.com",
            "https://www.google.com",
            "https://www.stackoverflow.com",
            "https://www.wikipedia.org"
        ]
        
        # Hàng đợi URLs để crawl
        url_queue = queue.Queue()
        for i in range(num_pages):
            url_queue.put(random.choice(urls))
        
        # Kết quả crawl
        results = []
        result_lock = threading.Lock()
        
        def crawler_worker():
            while not url_queue.empty():
                try:
                    url = url_queue.get(block=False)
                    
                    logger.info(f"Crawling {url}")
                    start_time = time.time()
                    
                    req = urllib.request.Request(
                        url, 
                        headers={'User-Agent': 'Mozilla/5.0 AMD Diagnostics Tool'}
                    )
                    
                    with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
                        data = response.read(50000)  # Đọc 50KB
                        
                    execution_time = time.time() - start_time
                    
                    with result_lock:
                        results.append({
                            'url': url,
                            'success': True,
                            'time': execution_time,
                            'size': len(data)
                        })
                    
                    # Tạm dừng ngắn để không tải quá nhanh
                    time.sleep(0.5)
                    
                except queue.Empty:
                    break
                    
                except Exception as e:
                    logger.error(f"Lỗi khi crawl {url}: {e}")
                    with result_lock:
                        results.append({
                            'url': url,
                            'success': False,
                            'error': str(e)
                        })
        
        # Tạo và chạy threads
        start_time = time.time()
        threads = []
        for i in range(concurrency):
            thread = threading.Thread(target=crawler_worker)
            threads.append(thread)
            thread.start()
        
        # Đợi tất cả threads hoàn thành
        for thread in threads:
            thread.join()
            
        execution_time = time.time() - start_time
        
        # Phân tích kết quả
        success_count = sum(1 for r in results if r['success'])
        logger.info(f"Crawl hoàn tất: {success_count}/{len(results)} thành công trong {execution_time:.2f}s")
        
        return {
            'pages': len(results),
            'successful': success_count,
            'execution_time': execution_time,
            'details': results
        }

def run_diagnostics():
    """Chạy tất cả các bài kiểm tra chẩn đoán"""
    results = {}
    start_time = time.time()
    
    logger.info("=== CHẨN ĐOÁN SỰ CỐ AMD BẮT ĐẦU ===")
    logger.info(f"Thời gian bắt đầu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Thu thập thông tin CPU
        logger.info("=== KIỂM TRA THÔNG TIN CPU ===")
        results['cpu_info'] = CPUInfo.get_cpu_info()
        
        # Kiểm tra bộ nhớ
        logger.info("\n=== KIỂM TRA BỘ NHỚ ===")
        results['memory_usage'] = MemoryTest.monitor_memory_usage(duration=5)
        results['memory_allocation'] = MemoryTest.test_memory_allocation()
        results['memory_fragmentation'] = MemoryTest.test_memory_fragmentation()
        
        # Kiểm tra xử lý đồng thời
        logger.info("\n=== KIỂM TRA XỬ LÝ ĐỒNG THỜI ===")
        results['threading'] = ConcurrencyTest.test_threading()
        results['multiprocessing'] = ConcurrencyTest.test_multiprocessing()
        results['race_conditions'] = ConcurrencyTest.test_race_conditions()
        
        # Kiểm tra tập lệnh CPU
        logger.info("\n=== KIỂM TRA TẬP LỆNH CPU ===")
        results['avx_test'] = CPUInstructionTest.test_avx()
        results['sse_test'] = CPUInstructionTest.test_sse()
        
        # Kiểm tra mạng
        logger.info("\n=== KIỂM TRA MẠNG ===")
        results['simple_networking'] = NetworkTest.test_simple_networking()
        results['concurrent_connections'] = NetworkTest.test_concurrent_connections()
        
        # Kiểm tra file I/O
        logger.info("\n=== KIỂM TRA FILE I/O ===")
        results['file_operations'] = FileIOTest.test_file_operations()
        results['concurrent_file_access'] = FileIOTest.test_concurrent_file_access()
        
        # Thử mô phỏng crawler
        logger.info("\n=== MÔ PHỎNG CRAWLER ===")
        results['crawler_simulation'] = SimulateCrawler.run_simple_crawler()
        
        # Lưu kết quả
        with open('amd_diagnostics_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
            
        logger.info(f"\nĐã lưu kết quả vào file amd_diagnostics_results.json")
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình chẩn đoán: {e}")
        logger.error(traceback.format_exc())
        
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"=== CHẨN ĐOÁN KẾT THÚC SAU {execution_time:.2f} GIÂY ===")
        
    return results

def analyze_results(results):
    """Phân tích kết quả và đưa ra các khuyến nghị"""
    logger.info("\n=== PHÂN TÍCH KẾT QUẢ ===")
    
    issues = []
    recommendations = []
    
    # Phân tích thông tin CPU
    if results.get('cpu_info'):
        cpu_info = results['cpu_info']
        is_amd = cpu_info.get('is_amd', False)
        logger.info(f"CPU {'là' if is_amd else 'không phải'} AMD")
        
        # Kiểm tra bộ lệnh CPU
        extensions = cpu_info.get('extensions', {})
        missing_extensions = []
        for ext in ['avx', 'avx2', 'sse4_1', 'sse4_2', 'fma']:
            if ext in extensions and not extensions[ext]:
                missing_extensions.append(ext)
        
        if missing_extensions:
            issues.append(f"CPU thiếu các bộ lệnh: {', '.join(missing_extensions)}")
            recommendations.append("Thêm tùy chọn để tắt sử dụng các bộ lệnh nâng cao (như AVX, AVX2) trong mã nguồn.")
    
    # Phân tích kết quả bộ nhớ
    if results.get('memory_fragmentation'):
        mem_frag = results['memory_fragmentation']
        if isinstance(mem_frag, dict) and 'ratio' in mem_frag and mem_frag['ratio'] > 1.3:
            issues.append(f"Phát hiện phân mảnh bộ nhớ (tỷ lệ {mem_frag['ratio']:.2f})")
            recommendations.append("Tăng tần suất gọi gc.collect() và kiểm tra rò rỉ bộ nhớ.")
    
    # Phân tích kết quả đa luồng
    if results.get('threading'):
        threading_results = results['threading']
        if threading_results['expected'] != threading_results['actual']:
            issues.append("Phát hiện vấn đề đồng bộ hóa luồng")
            recommendations.append("Đảm bảo sử dụng lock khi truy cập tài nguyên chia sẻ.")
    
    # Phân tích race conditions
    if results.get('race_conditions'):
        race_results = results['race_conditions']
        if race_results['errors_detected'] > 0:
            issues.append(f"Phát hiện {race_results['errors_detected']} lỗi điều kiện đua")
            recommendations.append("Sử dụng lock và cấu trúc đồng bộ hóa đúng cách.")
    
    # Phân tích kết quả kiểm tra AVX/SSE
    if results.get('avx_test') and not results['avx_test'].get('success', True):
        issues.append("Lỗi trong kiểm tra hướng dẫn AVX")
        recommendations.append("Sử dụng tùy chọn để tắt AVX/SSE trong thư viện NumPy/SciPy nếu được sử dụng.")
    
    # Phân tích kết quả mạng
    if results.get('concurrent_connections'):
        conn_results = results['concurrent_connections']
        failed_conns = [r for r in conn_results if not r.get('success', False)]
        if failed_conns:
            issues.append(f"Có {len(failed_conns)} trong số {len(conn_results)} kết nối đồng thời thất bại")
            recommendations.append("Giới hạn số lượng kết nối đồng thời và thêm retry logic.")
    
    # Phân tích kết quả I/O file
    if results.get('concurrent_file_access') and not results['concurrent_file_access'].get('success', False):
        issues.append("Phát hiện vấn đề khi truy cập file đồng thời")
        recommendations.append("Sử dụng file lock và kiểm tra xử lý ngoại lệ khi thao tác file.")
    
    # Phân tích kết quả giả lập crawler
    if results.get('crawler_simulation'):
        crawler_results = results['crawler_simulation']
        if crawler_results['successful'] < crawler_results['pages']:
            issues.append(f"{crawler_results['pages'] - crawler_results['successful']} tác vụ crawl thất bại")
            recommendations.append("Thêm xử lý ngoại lệ và retry logic cho crawler.")
    
    # Tóm tắt
    if issues:
        logger.info("\n=== PHÁT HIỆN CÁC VẤN ĐỀ ===")
        for i, issue in enumerate(issues):
            logger.info(f"{i+1}. {issue}")
    else:
        logger.info("\nKhông phát hiện vấn đề nghiêm trọng.")
    
    if recommendations:
        logger.info("\n=== KHUYẾN NGHỊ ===")
        for i, rec in enumerate(recommendations):
            logger.info(f"{i+1}. {rec}")
    
    # Các khuyến nghị chung cho AMD
    logger.info("\n=== KHUYẾN NGHỊ CHUNG CHO AMD ===")
    logger.info("1. Tắt hoặc giảm việc sử dụng các bộ lệnh AVX/AVX2/FMA trên AMD")
    logger.info("2. Sử dụng các biến môi trường như NPY_DISABLE_CPU_FEATURES='AVX2,FMA3' nếu dùng NumPy")
    logger.info("3. Thêm xử lý ngoại lệ kỹ lưỡng")
    logger.info("4. Giới hạn số lượng thread/task đồng thời")
    logger.info("5. Giải phóng bộ nhớ thường xuyên với gc.collect()")
    logger.info("6. Thêm timeout cho tất cả các tác vụ mạng và I/O")
    
    return {
        'issues': issues,
        'recommendations': recommendations
    }

def create_patch_code():
    """Tạo mã nguồn patch để khắc phục các vấn đề phổ biến trên AMD"""
    patch_code = """
# -*- coding: utf-8 -*-
\"\"\"
AMD Compatibility Patch
Một bộ các patch và wrapper để giúp code chạy ổn định trên hệ thống AMD
\"\"\"

import os
import sys
import gc
import threading
import time
import logging
import traceback
from functools import wraps
from contextlib import contextmanager

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("amd_patch.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("amd_patch")

def apply_cpu_patches():
    \"\"\"Áp dụng các patch cho CPU AMD\"\"\"
    logger.info("Áp dụng các patch cho CPU AMD...")
    
    # Tắt các bộ lệnh AVX/AVX2 có thể gây vấn đề
    os.environ['NPY_DISABLE_CPU_FEATURES'] = 'AVX2,AVX512F,FMA3'
    
    # Giới hạn số lượng thread cho các thư viện song song
    os.environ['OMP_NUM_THREADS'] = '1'
    os.environ['MKL_NUM_THREADS'] = '1'
    os.environ['NUMEXPR_NUM_THREADS'] = '1'
    
    # Tối ưu hóa memory allocator nếu có thể
    try:
        import numpy as np
        np.__config__.show()
    except:
        pass
        
    return True

def apply_memory_patches():
    \"\"\"Áp dụng các patch cho quản lý bộ nhớ\"\"\"
    logger.info("Áp dụng các patch cho quản lý bộ nhớ...")
    
    # Thiết lập GC chạy thường xuyên hơn
    gc.set_threshold(100, 5, 5)
    
    # Giới hạn kích thước stack của thread
    threading.stack_size(128 * 1024)  # 128KB thay vì mặc định
    
    # Tạo thread dọn dẹp bộ nhớ định kỳ
    def memory_cleaner():
        while True:
            gc.collect()
            time.sleep(60)  # 1 phút
    
    cleaner = threading.Thread(target=memory_cleaner, daemon=True)
    cleaner.start()
    
    return True

@contextmanager
def timeout(seconds, error_message="Thao tác vượt quá thời gian chờ"):
    \"\"\"Context manager để giới hạn thời gian thực thi\"\"\"
    import signal
    
    def timeout_handler(signum, frame):
        raise TimeoutError(error_message)
    
    # Đăng ký handler (không hoạt động trên Windows)
    if sys.platform != 'win32':
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(seconds)
    
    try:
        yield
    finally:
        if sys.platform != 'win32':
            signal.alarm(0)

def safe_thread(func):
    \"\"\"Decorator để bắt lỗi trong thread\"\"\"
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            thread_name = threading.current_thread().name
            logger.error(f"Lỗi trong thread {thread_name}: {e}")
            logger.error(traceback.format_exc())
    return wrapper

def safe_crawler(func):
    \"\"\"Decorator cho các hàm crawl\"\"\"
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            with timeout(60, "Crawl timeout"):
                return func(*args, **kwargs)
        except TimeoutError as e:
            logger.error(f"Crawl timeout: {e}")
            return None
        except Exception as e:
            logger.error(f"Lỗi trong crawl: {e}")
            logger.error(traceback.format_exc())
            return None
    return wrapper

def patch_requests():
    \"\"\"Patch module requests nếu được sử dụng\"\"\"
    try:
        import requests
        
        original_request = requests.request
        @wraps(original_request)
        def safe_request(*args, **kwargs):
            # Đảm bảo có timeout
            if 'timeout' not in kwargs:
                kwargs['timeout'] = 30
            
            try:
                return original_request(*args, **kwargs)
            except Exception as e:
                logger.error(f"Lỗi requests: {e}")
                # Tạo response giả để tránh crash hoàn toàn
                res = requests.Response()
                res.status_code = 500
                res._content = str(e).encode('utf-8')
                return res
                
        requests.request = safe_request
        logger.info("Đã patch requests.request")
    except ImportError:
        pass

def patch_urllib():
    \"\"\"Patch module urllib nếu được sử dụng\"\"\"
    try:
        import urllib.request
        
        original_urlopen = urllib.request.urlopen
        @wraps(original_urlopen)
        def safe_urlopen(*args, **kwargs):
            # Đảm bảo có timeout
            if 'timeout' not in kwargs:
                kwargs['timeout'] = 30
                
            try:
                return original_urlopen(*args, **kwargs)
            except Exception as e:
                logger.error(f"Lỗi urllib: {e}")
                raise  # Vẫn raise để code gọi có thể xử lý
                
        urllib.request.urlopen = safe_urlopen
        logger.info("Đã patch urllib.request.urlopen")
    except ImportError:
        pass

def limit_threads(max_workers=5):
    \"\"\"Giới hạn số lượng thread hoạt động cùng lúc\"\"\"
    from concurrent.futures import ThreadPoolExecutor
    executor = ThreadPoolExecutor(max_workers=max_workers)
    return executor

def safe_thread_pool(func, args_list, max_workers=5, timeout=60):
    \"\"\"Chạy các tác vụ trong thread pool với xử lý lỗi\"\"\"
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_args = {executor.submit(func, *args): args for args in args_list}
        for future in as_completed(future_to_args):
            args = future_to_args[future]
            try:
                result = future.result(timeout=timeout)
                results.append((args, result, None))
            except Exception as e:
                results.append((args, None, str(e)))
                logger.error(f"Lỗi task {func.__name__} với args {args}: {e}")
    
    return results

def apply_all_patches():
    \"\"\"Áp dụng tất cả các patch\"\"\"
    logger.info("Bắt đầu áp dụng tất cả các patch cho AMD...")
    
    # Cài đặt mã hóa UTF-8
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    
    # Áp dụng các patch
    apply_cpu_patches()
    apply_memory_patches()
    patch_requests()
    patch_urllib()
    
    logger.info("Đã áp dụng tất cả các patch thành công")
    
    return True

# Ví dụ sử dụng
if __name__ == "__main__":
    print("=== AMD Compatibility Patch ===")
    apply_all_patches()
    print("Các patch đã được áp dụng thành công!")
    print("Hướng dẫn sử dụng:")
    print("1. Import module này trong code chính của bạn:")
    print("   import amd_patch")
    print("2. Gọi hàm để áp dụng tất cả các patch:")
    print("   amd_patch.apply_all_patches()")
    print("3. Sử dụng các decorator cho các hàm cụ thể:")
    print("   @amd_patch.safe_thread")
    print("   def my_thread_function():")
    print("       # code here")
    print("")
    print("   @amd_patch.safe_crawler")
    print("   def my_crawler_function(url):")
    print("       # crawler code here")
"""
    
    # Lưu patch code vào file
    with open('amd_patch.py', 'w', encoding='utf-8') as f:
        f.write(patch_code)
        
    logger.info("Đã tạo mã nguồn patch và lưu vào file amd_patch.py")
    logger.info("Hãy import module này trong code chính của bạn và sử dụng các patch được cung cấp.")

def main():
    """Hàm chính để chạy tất cả các chẩn đoán"""
    try:
        # Chạy công cụ chẩn đoán
        results = run_diagnostics()
        
        # Phân tích kết quả
        analysis = analyze_results(results)
        
        # Tạo mã nguồn patch
        create_patch_code()
        
        logger.info("\n=== HOÀN THÀNH ===")
        logger.info("1. Kết quả đã được lưu trong amd_diagnostics_results.json")
        logger.info("2. Một bộ patch đã được tạo ra trong amd_patch.py")
        logger.info("3. Log chi tiết được lưu trong amd_diagnostics.log")
        
        print("\nCông cụ chẩn đoán đã hoàn thành! Vui lòng kiểm tra:")
        print("1. amd_diagnostics_results.json - Kết quả chi tiết")
        print("2. amd_patch.py - Mã nguồn để sửa lỗi")
        print("3. amd_diagnostics.log - Log chi tiết")
        
    except Exception as e:
        logger.critical(f"Lỗi nghiêm trọng trong quá trình chẩn đoán: {e}")
        logger.critical(traceback.format_exc())
        print(f"Đã xảy ra lỗi: {e}")
        print("Vui lòng kiểm tra file amd_diagnostics.log để biết thêm chi tiết.")

if __name__ == "__main__":
    main()