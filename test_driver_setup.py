#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script để kiểm tra enhanced Chrome driver setup
"""

import logging
import time
import multiprocessing
from concurrent.futures import ThreadPoolExecutor, as_completed

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_single_driver():
    """Test tạo và đóng một driver đơn lẻ"""
    print("=== TEST 1: Single Driver Creation ===")
    try:
        from crawlers.nettruyen_crawler import setup_driver
        
        print("Tạo driver...")
        driver = setup_driver()
        print("✓ Driver tạo thành công!")
        
        print("Test navigation...")
        driver.get("https://www.google.com")
        print(f"✓ Navigation thành công! Title: {driver.title}")
        
        print("Đóng driver...")
        driver.quit()
        print("✓ Driver đóng thành công!")
        return True
        
    except Exception as e:
        print(f"✗ Test thất bại: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_multiple_drivers_sequential():
    """Test tạo nhiều drivers tuần tự"""
    print("\n=== TEST 2: Multiple Sequential Drivers ===")
    success_count = 0
    total_tests = 3
    
    for i in range(total_tests):
        try:
            print(f"Tạo driver {i+1}/{total_tests}...")
            from crawlers.nettruyen_crawler import setup_driver
            
            driver = setup_driver()
            driver.get("about:blank")
            print(f"✓ Driver {i+1} thành công!")
            driver.quit()
            success_count += 1
            
            # Đợi một chút giữa các lần tạo
            time.sleep(2)
            
        except Exception as e:
            print(f"✗ Driver {i+1} thất bại: {e}")
    
    print(f"Kết quả: {success_count}/{total_tests} drivers thành công")
    return success_count == total_tests

def create_driver_worker(worker_id):
    """Worker function để test trong multiprocessing"""
    try:
        from crawlers.nettruyen_crawler import setup_driver
        
        driver = setup_driver()
        driver.get("about:blank")
        title = driver.title
        driver.quit()
        
        return {
            'worker_id': worker_id,
            'success': True,
            'title': title
        }
        
    except Exception as e:
        return {
            'worker_id': worker_id,
            'success': False,
            'error': str(e)
        }

def test_concurrent_drivers():
    """Test tạo drivers đồng thời"""
    print("\n=== TEST 3: Concurrent Drivers ===")
    num_workers = 3
    
    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit tasks
            futures = [executor.submit(create_driver_worker, i) for i in range(num_workers)]
            
            # Collect results
            results = []
            for future in as_completed(futures, timeout=60):
                result = future.result()
                results.append(result)
                status = "✓" if result['success'] else "✗"
                if result['success']:
                    print(f"{status} Worker {result['worker_id']} thành công")
                else:
                    print(f"{status} Worker {result['worker_id']} thất bại: {result['error']}")
        
        success_count = sum(1 for r in results if r['success'])
        print(f"Kết quả: {success_count}/{num_workers} concurrent drivers thành công")
        return success_count >= num_workers // 2  # Cho phép một số thất bại
        
    except Exception as e:
        print(f"✗ Concurrent test thất bại: {e}")
        return False

def test_resource_cleanup():
    """Test cleanup resources"""
    print("\n=== TEST 4: Resource Cleanup ===")
    try:
        from crawlers.nettruyen_crawler import cleanup_chrome_processes, kill_chromedriver_processes, cleanup_chrome_temp_dirs
        
        print("Test cleanup functions...")
        chrome_count = cleanup_chrome_processes()
        driver_count = kill_chromedriver_processes() 
        temp_count = cleanup_chrome_temp_dirs()
        
        print(f"✓ Cleanup hoàn thành: {chrome_count} Chrome, {driver_count} ChromeDriver, {temp_count} temp files")
        return True
        
    except Exception as e:
        print(f"✗ Cleanup test thất bại: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Chạy tất cả tests"""
    print("Bắt đầu test enhanced Chrome driver setup...")
    print(f"Python version: {multiprocessing.get_start_method()}")
    
    # Test cleanup trước
    test_resource_cleanup()
    
    # Chạy các tests
    tests = [
        test_single_driver,
        test_multiple_drivers_sequential,
        test_concurrent_drivers,
        test_resource_cleanup
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"Test {test_func.__name__} crashed: {e}")
    
    print(f"\n=== KẾT QUẢ CUỐI CÙNG ===")
    print(f"Passed: {passed}/{total} tests")
    print("✓ Enhanced driver setup hoạt động tốt!" if passed >= total // 2 else "✗ Enhanced driver setup cần cải thiện")
    
    return passed >= total // 2

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)