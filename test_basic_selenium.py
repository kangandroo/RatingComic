#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simple test script để kiểm tra SeleniumBase basic functionality
"""

import logging
import os

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_basic_seleniumbase():
    """Test basic SeleniumBase driver creation"""
    print("=== Testing Basic SeleniumBase Driver ===")
    
    try:
        # Set environment variables
        os.environ['PYTHONWARNINGS'] = 'ignore'
        
        print("1. Importing SeleniumBase...")
        from seleniumbase import Driver
        print("✓ SeleniumBase imported successfully")
        
        print("2. Creating basic driver...")
        driver = Driver(browser="chrome", headless=True)
        print("✓ Driver created successfully")
        
        print("3. Testing navigation...")
        driver.get("about:blank")
        print("✓ Navigation successful")
        
        print("4. Closing driver...")
        driver.quit()
        print("✓ Driver closed successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_chrome_availability():
    """Test if Chrome is available"""
    print("\n=== Testing Chrome Availability ===")
    
    try:
        import subprocess
        
        # Check if chrome is available
        result = subprocess.run(['which', 'google-chrome'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✓ Chrome found at: {result.stdout.strip()}")
        else:
            result = subprocess.run(['which', 'chromium-browser'], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✓ Chromium found at: {result.stdout.strip()}")
            else:
                print("✗ Chrome/Chromium not found in PATH")
                return False
        
        # Check version
        try:
            version_result = subprocess.run(['google-chrome', '--version'], capture_output=True, text=True, timeout=10)
            if version_result.returncode == 0:
                print(f"✓ Chrome version: {version_result.stdout.strip()}")
            else:
                version_result = subprocess.run(['chromium-browser', '--version'], capture_output=True, text=True, timeout=10)
                if version_result.returncode == 0:
                    print(f"✓ Chromium version: {version_result.stdout.strip()}")
        except Exception as e:
            print(f"Warning: Could not get version: {e}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error checking Chrome: {e}")
        return False

def main():
    """Run all tests"""
    print("Starting basic SeleniumBase diagnostics...")
    
    # Test Chrome availability first
    chrome_ok = test_chrome_availability()
    
    # Test SeleniumBase if Chrome is available
    if chrome_ok:
        sb_ok = test_basic_seleniumbase()
    else:
        print("Skipping SeleniumBase test due to Chrome issues")
        sb_ok = False
    
    print(f"\n=== Results ===")
    print(f"Chrome available: {'✓' if chrome_ok else '✗'}")
    print(f"SeleniumBase working: {'✓' if sb_ok else '✗'}")
    
    return chrome_ok and sb_ok

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)