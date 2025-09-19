#!/usr/bin/env python3
"""
Debug script to test all API endpoints and identify issues
Run this to diagnose problems with your Flask app
"""

import requests
import json
import time
from io import BytesIO

API_BASE = 'http://localhost:5000/api/v1'

def test_health():
    """Test the health endpoint"""
    print("🔍 Testing health endpoint...")
    try:
        response = requests.get(f'{API_BASE}/health', timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def test_login():
    """Test user login"""
    print("\n🔍 Testing login endpoint...")
    try:
        response = requests.post(
            f'{API_BASE}/auth/login',
            json={'username': 'admin', 'password': 'admin123'},
            timeout=5
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Token received: {data.get('token', 'No token')[:50]}...")
            return data.get('token')
        else:
            print(f"   Error: {response.text}")
            return None
    except Exception as e:
        print(f"   ERROR: {e}")
        return None

def test_videos_list(token):
    """Test getting videos list"""
    print("\n🔍 Testing videos list endpoint...")
    try:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(f'{API_BASE}/videos', headers=headers, timeout=5)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Videos found: {len(data.get('videos', []))}")
            print(f"   Pagination: {data.get('pagination', {})}")
            return True
        else:
            print(f"   Error: {response.text}")
            return False
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def test_video_upload(token):
    """Test video upload"""
    print("\n🔍 Testing video upload endpoint...")
    try:
        # Create a small dummy video file
        dummy_data = b'FAKE_VIDEO_DATA' * 1000  # 14KB fake video
        files = {'video': ('test_video.mp4', BytesIO(dummy_data), 'video/mp4')}
        headers = {'Authorization': f'Bearer {token}'}
        
        response = requests.post(
            f'{API_BASE}/videos',
            files=files,
            headers=headers,
            timeout=10
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 201:
            data = response.json()
            print(f"   Upload successful: {data.get('message')}")
            video_id = data.get('video', {}).get('id')
            print(f"   Video ID: {video_id}")
            return video_id
        else:
            print(f"   Error: {response.text}")
            return None
    except Exception as e:
        print(f"   ERROR: {e}")
        return None

def test_transcoding(token, video_id):
    """Test video transcoding"""
    if not video_id:
        print("\n❌ Skipping transcoding test - no video ID")
        return None
        
    print(f"\n🔍 Testing transcoding endpoint for video {video_id}...")
    try:
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        data = {
            'target_format': 'h264',
            'target_resolution': '720p',
            'target_bitrate': '2M'
        }
        
        response = requests.post(
            f'{API_BASE}/videos/{video_id}/transcode',
            json=data,
            headers=headers,
            timeout=10
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 202:
            job_data = response.json()
            print(f"   Job created: {job_data.get('job_id')}")
            return job_data.get('job_id')
        else:
            print(f"   Error: {response.text}")
            return None
    except Exception as e:
        print(f"   ERROR: {e}")
        return None

def test_jobs_list(token):
    """Test getting transcoding jobs"""
    print("\n🔍 Testing transcoding jobs endpoint...")
    try:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(f'{API_BASE}/transcoding/jobs', headers=headers, timeout=5)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Jobs found: {len(data.get('jobs', []))}")
            return True
        else:
            print(f"   Error: {response.text}")
            return False
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def test_stress_test(token):
    """Test CPU stress test endpoint"""
    print("\n🔍 Testing stress test endpoint...")
    try:
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        data = {'duration': 10, 'intensity': 2}  # Short test
        
        response = requests.post(
            f'{API_BASE}/stress-test',
            json=data,
            headers=headers,
            timeout=15
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"   Stress test started: {result.get('message')}")
            return True
        else:
            print(f"   Error: {response.text}")
            return False
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def test_stats(token):
    """Test stats endpoint"""
    print("\n🔍 Testing stats endpoint...")
    try:
        headers = {'Authorization': f'Bearer {token}'}
        response = requests.get(f'{API_BASE}/stats', headers=headers, timeout=5)
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   Stats: {json.dumps(data, indent=2)}")
            return True
        else:
            print(f"   Error: {response.text}")
            return False
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def main():
    print("🚀 Video Transcoding API Debug Tool")
    print("=" * 50)
    
    # Test basic connectivity
    if not test_health():
        print("\n❌ Health check failed. Is your Flask app running on localhost:5000?")
        return
    
    # Test authentication
    token = test_login()
    if not token:
        print("\n❌ Login failed. Check your user credentials in the database.")
        return
    
    # Test core functionality
    test_videos_list(token)
    video_id = test_video_upload(token)
    test_transcoding(token, video_id)
    test_jobs_list(token)
    test_stress_test(token)
    test_stats(token)
    
    print("\n" + "=" * 50)
    print("🏁 Debug test completed!")
    print("\nIf any tests failed, check the Flask app logs for detailed error messages.")
    print("Common issues:")
    print("  - Database file permissions")
    print("  - Missing FFmpeg installation")
    print("  - CORS configuration")
    print("  - File upload directory permissions")

if __name__ == "__main__":
    main()