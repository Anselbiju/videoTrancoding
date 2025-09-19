#!/usr/bin/env python3
"""
Load testing script for the Flask Video Transcoding API
This script can generate enough load to stress test multiple servers
"""

import requests
import json
import time
import threading
import concurrent.futures
from io import BytesIO
import random
import os
import argparse
import tempfile

class VideoTranscodingLoadTester:
    def __init__(self, base_url, num_threads=20, duration=300):
        self.base_url = base_url.rstrip('/')
        self.num_threads = num_threads
        self.duration = duration
        self.session = requests.Session()
        self.tokens = []
        self.uploaded_videos = []
        self.stats = {
            'requests_sent': 0,
            'requests_successful': 0,
            'requests_failed': 0,
            'total_response_time': 0,
            'uploads_successful': 0,
            'transcoding_jobs_created': 0,
            'stress_tests_started': 0
        }
        self.lock = threading.Lock()
    
    def create_test_video(self, duration_seconds=10):
        """Create a test video file using FFmpeg"""
        try:
            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(suffix='.mp4', delete=False)
            temp_file.close()
            
            # Generate a simple test video using FFmpeg
            cmd = [
                'ffmpeg', '-f', 'lavfi', '-i', 
                f'testsrc=duration={duration_seconds}:size=640x480:rate=30',
                '-f', 'lavfi', '-i', 'sine=frequency=1000:duration={}'.format(duration_seconds),
                '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
                '-c:a', 'aac', '-shortest', '-y', temp_file.name
            ]
            
            import subprocess
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                with open(temp_file.name, 'rb') as f:
                    video_data = f.read()
                os.unlink(temp_file.name)
                return BytesIO(video_data)
            else:
                print(f"FFmpeg error: {result.stderr}")
                os.unlink(temp_file.name)
                return None
                
        except Exception as e:
            print(f"Error creating test video: {e}")
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            return None
    
    def create_dummy_video_data(self):
        """Create dummy video data if FFmpeg is not available"""
        # Create a minimal valid MP4 file header
        dummy_data = b'\x00\x00\x00\x20ftypmp41\x00\x00\x00\x00mp41isom' + b'\x00' * 1000
        return BytesIO(dummy_data)
    
    def login_user(self, username="admin", password="admin123"):
        """Login and get JWT token"""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/auth/login",
                json={"username": username, "password": password},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                return data.get('token')
        except Exception as e:
            print(f"Login failed: {e}")
        return None
    
    def setup_users(self):
        """Setup multiple user tokens for testing"""
        users = [
            ("admin", "admin123"),
            ("creator1", "creator123"),
            ("user1", "user123"),
            ("user2", "user123")
        ]
        
        for username, password in users:
            token = self.login_user(username, password)
            if token:
                self.tokens.append(token)
                print(f"✓ Logged in as {username}")
            else:
                print(f"✗ Failed to login as {username}")
        
        if not self.tokens:
            print("ERROR: No valid tokens obtained. Cannot proceed with load testing.")
            return False
        
        print(f"Setup complete with {len(self.tokens)} user tokens")
        return True
    
    def upload_video(self, token):
        """Upload a test video"""
        try:
            # Try to create a real video first, fallback to dummy data
            video_data = self.create_test_video(5)  # 5 second video
            if video_data is None:
                video_data = self.create_dummy_video_data()
            
            files = {'video': ('test_video.mp4', video_data, 'video/mp4')}
            headers = {'Authorization': f'Bearer {token}'}
            
            start_time = time.time()
            response = self.session.post(
                f"{self.base_url}/api/v1/videos",
                files=files,
                headers=headers,
                timeout=60
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 201:
                data = response.json()
                video_id = data.get('video', {}).get('id')
                if video_id:
                    with self.lock:
                        self.uploaded_videos.append(video_id)
                        self.stats['uploads_successful'] += 1
                        self.stats['requests_successful'] += 1
                return video_id
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"Upload error: {e}")
        return None
    
    def start_transcoding(self, token, video_id):
        """Start video transcoding job"""
        transcoding_options = [
            {'target_format': 'h264', 'target_resolution': '720p', 'target_bitrate': '2M'},
            {'target_format': 'h264', 'target_resolution': '1080p', 'target_bitrate': '4M'},
            {'target_format': 'h265', 'target_resolution': '720p', 'target_bitrate': '1.5M'},
            {'target_format': 'h265', 'target_resolution': '1080p', 'target_bitrate': '3M'},
            {'target_format': 'vp9', 'target_resolution': '720p', 'target_bitrate': '2M'}
        ]
        
        try:
            transcode_data = random.choice(transcoding_options)
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            
            start_time = time.time()
            response = self.session.post(
                f"{self.base_url}/api/v1/videos/{video_id}/transcode",
                json=transcode_data,
                headers=headers,
                timeout=30
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 202:
                with self.lock:
                    self.stats['transcoding_jobs_created'] += 1
                    self.stats['requests_successful'] += 1
                return response.json().get('job_id')
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"Transcoding error: {e}")
        return None
    
    def check_transcoding_status(self, token, job_id):
        """Check transcoding job status"""
        try:
            headers = {'Authorization': f'Bearer {token}'}
            
            start_time = time.time()
            response = self.session.get(
                f"{self.base_url}/api/v1/transcoding/jobs/{job_id}",
                headers=headers,
                timeout=10
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 200:
                with self.lock:
                    self.stats['requests_successful'] += 1
                return response.json()
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"Status check error: {e}")
        return None
    
    def get_videos_list(self, token):
        """Get user's videos list"""
        try:
            headers = {'Authorization': f'Bearer {token}'}
            params = {
                'page': random.randint(1, 3),
                'per_page': random.randint(5, 20),
                'sort_by': random.choice(['upload_time', 'file_size', 'original_filename']),
                'sort_order': random.choice(['ASC', 'DESC'])
            }
            
            start_time = time.time()
            response = self.session.get(
                f"{self.base_url}/api/v1/videos",
                headers=headers,
                params=params,
                timeout=10
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 200:
                with self.lock:
                    self.stats['requests_successful'] += 1
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"List videos error: {e}")
    
    def start_stress_test(self, token):
        """Call the dedicated stress test endpoint"""
        try:
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            data = {
                'duration': 60,  # 1 minute of CPU stress per call
                'intensity': 3   # 3 concurrent CPU tasks
            }
            
            start_time = time.time()
            response = self.session.post(
                f"{self.base_url}/api/v1/stress-test",
                json=data,
                headers=headers,
                timeout=30
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 200:
                with self.lock:
                    self.stats['stress_tests_started'] += 1
                    self.stats['requests_successful'] += 1
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"Stress test error: {e}")
    
    def batch_transcode(self, token, video_ids):
        """Start batch transcoding for multiple videos"""
        if not video_ids:
            return
            
        try:
            headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
            data = {
                'video_ids': video_ids[:3],  # Limit to 3 videos per batch
                'target_format': random.choice(['h264', 'h265']),
                'target_resolution': random.choice(['720p', '1080p']),
                'target_bitrate': random.choice(['2M', '4M'])
            }
            
            start_time = time.time()
            response = self.session.post(
                f"{self.base_url}/api/v1/transcoding/batch",
                json=data,
                headers=headers,
                timeout=30
            )
            response_time = time.time() - start_time
            
            with self.lock:
                self.stats['requests_sent'] += 1
                self.stats['total_response_time'] += response_time
            
            if response.status_code == 202:
                batch_data = response.json()
                with self.lock:
                    self.stats['transcoding_jobs_created'] += len(batch_data.get('job_ids', []))
                    self.stats['requests_successful'] += 1
            else:
                with self.lock:
                    self.stats['requests_failed'] += 1
                    
        except Exception as e:
            with self.lock:
                self.stats['requests_failed'] += 1
            print(f"Batch transcode error: {e}")
    
    def worker_thread(self, thread_id):
        """Main worker thread that performs various operations"""
        print(f"Thread {thread_id} started")
        
        start_time = time.time()
        local_videos = []
        local_jobs = []
        
        while time.time() - start_time < self.duration:
            try:
                token = random.choice(self.tokens)
                
                # Randomly choose operation type with weights for CPU-intensive operations
                operation = random.choices(
                    ['upload', 'transcode', 'batch_transcode', 'status_check', 'list_videos', 'stress_test'],
                    weights=[25, 35, 15, 10, 10, 5]  # Favor transcoding operations
                )[0]
                
                if operation == 'upload':
                    video_id = self.upload_video(token)
                    if video_id:
                        local_videos.append(video_id)
                
                elif operation == 'transcode' and (local_videos or self.uploaded_videos):
                    available_videos = local_videos if local_videos else list(self.uploaded_videos)
                    if available_videos:
                        video_id = random.choice(available_videos)
                        job_id = self.start_transcoding(token, video_id)
                        if job_id:
                            local_jobs.append(job_id)
                
                elif operation == 'batch_transcode' and (local_videos or self.uploaded_videos):
                    available_videos = local_videos if local_videos else list(self.uploaded_videos)
                    if available_videos:
                        batch_videos = random.sample(available_videos, min(3, len(available_videos)))
                        self.batch_transcode(token, batch_videos)
                
                elif operation == 'status_check' and local_jobs:
                    job_id = random.choice(local_jobs)
                    self.check_transcoding_status(token, job_id)
                
                elif operation == 'list_videos':
                    self.get_videos_list(token)
                
                elif operation == 'stress_test':
                    self.start_stress_test(token)
                
                # Random delay between requests
                time.sleep(random.uniform(0.2, 1.0))
                
            except Exception as e:
                print(f"Thread {thread_id} error: {e}")
                time.sleep(1)
        
        print(f"Thread {thread_id} completed")
    
    def run_load_test(self):
        """Run the complete load test"""
        print(f"Starting video transcoding load test with {self.num_threads} threads for {self.duration} seconds")
        print(f"Target: {self.base_url}")
        
        if not self.setup_users():
            return
        
        # Upload some initial videos for transcoding
        print("Uploading initial videos...")
        for _ in range(5):
            token = random.choice(self.tokens)
            self.upload_video(token)
        
        start_time = time.time()
        
        # Start worker threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [executor.submit(self.worker_thread, i) for i in range(self.num_threads)]
            
            # Monitor progress
            while time.time() - start_time < self.duration:
                time.sleep(15)
                elapsed = time.time() - start_time
                remaining = self.duration - elapsed
                
                with self.lock:
                    avg_response_time = (self.stats['total_response_time'] / 
                                       max(1, self.stats['requests_sent']))
                    success_rate = (self.stats['requests_successful'] / 
                                  max(1, self.stats['requests_sent']) * 100)
                    
                    print(f"\nProgress: {elapsed:.0f}s / {self.duration}s")
                    print(f"Requests: {self.stats['requests_sent']} "
                          f"(Success: {self.stats['requests_successful']}, "
                          f"Failed: {self.stats['requests_failed']})")
                    print(f"Success Rate: {success_rate:.1f}%")
                    print(f"Avg Response Time: {avg_response_time:.3f}s")
                    print(f"Videos Uploaded: {self.stats['uploads_successful']}")
                    print(f"Transcoding Jobs: {self.stats['transcoding_jobs_created']}")
                    print(f"Stress Tests: {self.stats['stress_tests_started']}")
            
            # Wait for all threads to complete
            concurrent.futures.wait(futures)
        
        # Final statistics
        print("\n" + "="*60)
        print("VIDEO TRANSCODING LOAD TEST COMPLETED")
        print("="*60)
        
        total_time = time.time() - start_time
        requests_per_second = self.stats['requests_sent'] / total_time
        avg_response_time = (self.stats['total_response_time'] / 
                           max(1, self.stats['requests_sent']))
        success_rate = (self.stats['requests_successful'] / 
                      max(1, self.stats['requests_sent']) * 100)
        
        print(f"Duration: {total_time:.1f} seconds")
        print(f"Total Requests: {self.stats['requests_sent']}")
        print(f"Successful Requests: {self.stats['requests_successful']}")
        print(f"Failed Requests: {self.stats['requests_failed']}")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"Requests per Second: {requests_per_second:.2f}")
        print(f"Average Response Time: {avg_response_time:.3f} seconds")
        print(f"Videos Uploaded: {self.stats['uploads_successful']}")
        print(f"Transcoding Jobs Created: {self.stats['transcoding_jobs_created']}")
        print(f"Stress Tests Started: {self.stats['stress_tests_started']}")
        
        if requests_per_second > 30:
            print("\n✓ EXCELLENT: High request rate achieved - suitable for multi-server load testing")
            print("  This load can easily stress multiple EC2 instances simultaneously")
        elif requests_per_second > 15:
            print("\n✓ GOOD: Moderate request rate - should be able to load multiple servers")
            print("  Consider increasing thread count for even higher load")
        else:
            print("\n⚠ LOW: Consider increasing thread count or optimizing for better load generation")
        
        print(f"\nCPU Load Generation Potential:")
        print(f"- Transcoding jobs will create sustained high CPU usage")
        print(f"- Each job can run for 30+ seconds with >90% CPU utilization")
        print(f"- Stress test endpoints provide additional CPU load")
        print(f"- Ready for multi-server deployment testing")

def main():
    parser = argparse.ArgumentParser(description='Load test the Video Transcoding API')
    parser.add_argument('--url', default='http://localhost:5000', 
                       help='Base URL of the API (default: http://localhost:5000)')
    parser.add_argument('--threads', type=int, default=15,
                       help='Number of concurrent threads (default: 15)')
    parser.add_argument('--duration', type=int, default=300,
                       help='Test duration in seconds (default: 300)')
    parser.add_argument('--quick', action='store_true',
                       help='Quick test mode (8 threads, 60 seconds)')
    parser.add_argument('--intensive', action='store_true',
                       help='Intensive test mode (25 threads, 600 seconds)')
    
    args = parser.parse_args()
    
    if args.quick:
        threads = 8
        duration = 60
    elif args.intensive:
        threads = 25
        duration = 600
    else:
        threads = args.threads
        duration = args.duration
    
    print("Video Transcoding API Load Tester")
    print("="*40)
    print("This tool will generate CPU-intensive video transcoding requests")
    print("Ensure FFmpeg is installed for optimal performance")
    print()
    
    tester = VideoTranscodingLoadTester(
        base_url=args.url,
        num_threads=threads,
        duration=duration
    )
    
    try:
        tester.run_load_test()
    except KeyboardInterrupt:
        print("\nLoad test interrupted by user")
    except Exception as e:
        print(f"\nLoad test failed: {e}")

if __name__ == "__main__":
    main()