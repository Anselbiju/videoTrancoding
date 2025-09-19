from flask import Flask, request, jsonify, send_file, send_from_directory
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
import jwt
from functools import wraps
import os
import json
import uuid
from datetime import datetime, timedelta, timezone
import time
import threading
import subprocess
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import logging
import hashlib

app = Flask(__name__)
app.config['SECRET_KEY'] = 'video-transcoding-secret-key-change-in-production'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['TRANSCODED_FOLDER'] = 'transcoded'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size

CORS(app)

# Ensure upload directories exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['TRANSCODED_FOLDER'], exist_ok=True)

# Thread pool for CPU-intensive transcoding tasks
executor = ThreadPoolExecutor(max_workers=4)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect('video_transcoder.db')
    cursor = conn.cursor()
    
    # Users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Videos table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            filename TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            duration REAL,
            resolution TEXT,
            format TEXT,
            upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'uploaded',
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # Transcoding jobs table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transcoding_jobs (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            video_id TEXT NOT NULL,
            target_format TEXT NOT NULL,
            target_resolution TEXT,
            target_bitrate TEXT,
            status TEXT DEFAULT 'queued',
            progress INTEGER DEFAULT 0,
            result_filename TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            processing_time REAL,
            error_message TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (video_id) REFERENCES videos (id)
        )
    ''')
    
    # User preferences table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER PRIMARY KEY,
            default_format TEXT DEFAULT 'mp4',
            default_resolution TEXT DEFAULT '720p',
            default_quality TEXT DEFAULT 'medium',
            notifications_enabled BOOLEAN DEFAULT 1,
            auto_delete_originals BOOLEAN DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')
    
    # Create default users
    cursor.execute('SELECT COUNT(*) FROM users')
    count_result = cursor.fetchone()
    if count_result and count_result[0] == 0:
        default_users = [
            ('admin', 'admin@videotranscoder.com', generate_password_hash('admin123'), 'admin'),
            ('creator1', 'creator1@videotranscoder.com', generate_password_hash('creator123'), 'creator'),
            ('user1', 'user1@videotranscoder.com', generate_password_hash('user123'), 'user'),
            ('user2', 'user2@videotranscoder.com', generate_password_hash('user123'), 'user')
        ]
        cursor.executemany(
            'INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)',
            default_users
        )
        
        # Create default preferences for users
        cursor.execute('SELECT id FROM users')
        user_ids = cursor.fetchall()
        for user_id, in user_ids:
            cursor.execute(
                'INSERT INTO user_preferences (user_id) VALUES (?)',
                (user_id,)
            )
    
    conn.commit()
    conn.close()

init_db()

# Authentication decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token:
            return jsonify({'message': 'Token is missing'}), 401
        
        try:
            if token.startswith('Bearer '):
                token = token[7:]
            data = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
            current_user_id = data['user_id']
            current_user_role = data.get('role', 'user')
        except jwt.ExpiredSignatureError:
            return jsonify({'message': 'Token has expired'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'message': 'Token is invalid'}), 401
        
        return f(current_user_id, current_user_role, *args, **kwargs)
    return decorated

# Helper functions
def get_db():
    return sqlite3.connect('video_transcoder.db')

def allowed_video_file(filename):
    ALLOWED_EXTENSIONS = {'mp4', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'webm', 'm4v'}
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def generate_unique_filename(original_filename):
    ext = original_filename.rsplit('.', 1)[1].lower()
    unique_id = str(uuid.uuid4())
    return f"{unique_id}.{ext}"

def get_video_info(filepath):
    """Extract video metadata using ffprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', '-show_streams', filepath
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            
            # Extract video stream info
            video_stream = None
            for stream in data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    video_stream = stream
                    break
            
            if video_stream:
                duration = float(data.get('format', {}).get('duration', 0))
                width = video_stream.get('width', 0)
                height = video_stream.get('height', 0)
                resolution = f"{width}x{height}" if width and height else "unknown"
                
                return {
                    'duration': duration,
                    'resolution': resolution,
                    'format': video_stream.get('codec_name', 'unknown')
                }
    except Exception as e:
        logger.error(f"Error extracting video info: {e}")
    
    return {'duration': 0, 'resolution': 'unknown', 'format': 'unknown'}

def check_ffmpeg_available():
    """Check if FFmpeg is available on the system"""
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, timeout=5)
        return True
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

# CPU-intensive transcoding functions
def transcode_video_cpu_intensive(input_path, output_path, target_format, target_resolution, target_bitrate, job_id):
    """Highly CPU-intensive video transcoding with multiple passes and filters"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        
        # Update job status to processing
        cursor.execute(
            'UPDATE transcoding_jobs SET status = ?, started_at = CURRENT_TIMESTAMP WHERE id = ?',
            ('processing', job_id)
        )
        conn.commit()
        
        start_time = time.time()
        
        # Build FFmpeg command for CPU-intensive transcoding
        cmd = ['ffmpeg', '-i', input_path, '-y']  # -y to overwrite output files
        
        # Add CPU-intensive video filters for maximum processing load
        video_filters = []
        
        # Scale to target resolution with high-quality algorithm
        if target_resolution:
            if target_resolution == '4K':
                video_filters.append('scale=3840:2160:flags=lanczos')
            elif target_resolution == '1080p':
                video_filters.append('scale=1920:1080:flags=lanczos')
            elif target_resolution == '720p':
                video_filters.append('scale=1280:720:flags=lanczos')
            elif target_resolution == '480p':
                video_filters.append('scale=854:480:flags=lanczos')
        
        # Add CPU-intensive enhancement filters
        video_filters.extend([
            'unsharp=5:5:1.0:5:5:0.0',  # Sharpening filter
            'eq=contrast=1.1:brightness=0.02:saturation=1.1',  # Color enhancement
            'hqdn3d=4:3:6:4.5',  # High-quality denoise (very CPU intensive)
        ])
        
        # Apply all video filters
        if video_filters:
            cmd.extend(['-vf', ','.join(video_filters)])
        
        # Video codec settings for maximum CPU usage
        if target_format.lower() == 'h264':
            cmd.extend([
                '-c:v', 'libx264',
                '-preset', 'veryslow',  # Maximum CPU usage preset
                '-crf', '18',  # High quality (CPU intensive)
                '-x264-params', 'me=umh:subme=10:ref=16:b-adapt=2:direct=auto:weightp=2'
            ])
        elif target_format.lower() == 'h265':
            cmd.extend([
                '-c:v', 'libx265',
                '-preset', 'veryslow',
                '-crf', '20',
                '-x265-params', 'me=3:subme=4:ref=6:b-adapt=2'
            ])
        elif target_format.lower() == 'vp9':
            cmd.extend([
                '-c:v', 'libvpx-vp9',
                '-crf', '20',
                '-b:v', '0',  # Constant quality mode
                '-cpu-used', '0'  # Slowest, highest quality
            ])
        
        # Audio processing
        cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
        
        # Bitrate control
        if target_bitrate:
            cmd.extend(['-b:v', target_bitrate])
        
        cmd.append(output_path)
        
        # Execute transcoding with progress monitoring
        logger.info(f"Starting transcoding job {job_id}")
        
        # Execute the command
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode == 0:
            processing_time = time.time() - start_time
            
            # Update job completion
            cursor.execute('''
                UPDATE transcoding_jobs 
                SET status = ?, progress = ?, completed_at = CURRENT_TIMESTAMP, processing_time = ?
                WHERE id = ?
            ''', ('completed', 100, processing_time, job_id))
            
            logger.info(f"Transcoding job {job_id} completed in {processing_time:.2f} seconds")
        else:
            cursor.execute(
                'UPDATE transcoding_jobs SET status = ?, error_message = ? WHERE id = ?',
                ('failed', process.stderr, job_id)
            )
            logger.error(f"Transcoding job {job_id} failed: {process.stderr}")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            'UPDATE transcoding_jobs SET status = ?, error_message = ? WHERE id = ?',
            ('failed', str(e), job_id)
        )
        conn.commit()
        conn.close()
        logger.error(f"Transcoding job {job_id} error: {str(e)}")

# API Routes

@app.route('/')
def index():
    """Serve the main web interface"""
    try:
        # Try to serve the HTML file from the same directory
        return send_from_directory('.', 'index.html')
    except FileNotFoundError:
        # If index.html doesn't exist, show API info
        return jsonify({
            'message': 'Video Transcoding Platform API',
            'version': '1.0.0',
            'endpoints': {
                'auth': '/api/v1/auth/login',
                'videos': '/api/v1/videos',
                'transcoding': '/api/v1/transcoding',
                'health': '/api/v1/health'
            },
            'note': 'Place index.html in the same directory as app.py to access the web interface',
            'web_client': 'Create index.html file for the complete web interface'
        })

@app.route('/api/v1/auth/register', methods=['POST'])
def register():
    data = request.get_json()
    
    if not data or not data.get('username') or not data.get('email') or not data.get('password'):
        return jsonify({'message': 'Missing required fields'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        password_hash = generate_password_hash(data['password'])
        role = data.get('role', 'user')
        
        cursor.execute(
            'INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, ?)',
            (data['username'], data['email'], password_hash, role)
        )
        
        user_id = cursor.lastrowid
        
        # Create default preferences
        cursor.execute('INSERT INTO user_preferences (user_id) VALUES (?)', (user_id,))
        
        conn.commit()
        
        return jsonify({'message': 'User created successfully'}), 201
    except sqlite3.IntegrityError:
        return jsonify({'message': 'Username or email already exists'}), 409
    finally:
        conn.close()

@app.route('/api/v1/auth/login', methods=['POST'])
def login():
    data = request.get_json()
    
    if not data or not data.get('username') or not data.get('password'):
        return jsonify({'message': 'Missing username or password'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        'SELECT id, username, password_hash, role FROM users WHERE username = ?',
        (data['username'],)
    )
    user = cursor.fetchone()
    conn.close()
    
    if user and check_password_hash(user[2], data['password']):
        token = jwt.encode({
            'user_id': user[0],
            'username': user[1],
            'role': user[3],
            'exp': datetime.now(timezone.utc) + timedelta(hours=24)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        
        return jsonify({
            'message': 'Login successful',
            'token': token,
            'user': {'id': user[0], 'username': user[1], 'role': user[3]}
        }), 200
    
    return jsonify({'message': 'Invalid credentials'}), 401

@app.route('/api/v1/videos', methods=['POST'])
@token_required
def upload_video(current_user_id, current_user_role):
    if 'video' not in request.files:
        return jsonify({'message': 'No video file provided'}), 400
    
    file = request.files['video']
    if file.filename == '':
        return jsonify({'message': 'No video selected'}), 400
    
    if file and allowed_video_file(file.filename):
        original_filename = secure_filename(file.filename)
        filename = generate_unique_filename(original_filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Get video metadata
        video_info = get_video_info(filepath)
        file_size = os.path.getsize(filepath)
        video_id = str(uuid.uuid4())
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO videos (id, user_id, filename, original_filename, file_size, 
                              duration, resolution, format)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (video_id, current_user_id, filename, original_filename, file_size,
              video_info['duration'], video_info['resolution'], video_info['format']))
        conn.commit()
        conn.close()
        
        return jsonify({
            'message': 'Video uploaded successfully',
            'video': {
                'id': video_id,
                'filename': original_filename,
                'size': file_size,
                'duration': video_info['duration'],
                'resolution': video_info['resolution'],
                'format': video_info['format']
            }
        }), 201
    
    return jsonify({'message': 'Invalid video file type'}), 400

@app.route('/api/v1/videos', methods=['GET'])
@token_required
def get_videos(current_user_id, current_user_role):
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    status_filter = request.args.get('status')
    sort_by = request.args.get('sort_by', 'upload_time')
    sort_order = request.args.get('sort_order', 'DESC')
    
    # Validate sort parameters
    valid_sort_fields = ['upload_time', 'original_filename', 'file_size', 'duration']
    if sort_by not in valid_sort_fields:
        sort_by = 'upload_time'
    
    if sort_order not in ['ASC', 'DESC']:
        sort_order = 'DESC'
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        # Build query
        if current_user_role == 'admin':
            base_query = '''
                SELECT id, filename, original_filename, file_size, duration, resolution, 
                       format, upload_time, status, user_id
                FROM videos
            '''
            count_query = 'SELECT COUNT(*) FROM videos'
            params = []
        else:
            base_query = '''
                SELECT id, filename, original_filename, file_size, duration, resolution, 
                       format, upload_time, status, user_id
                FROM videos WHERE user_id = ?
            '''
            count_query = 'SELECT COUNT(*) FROM videos WHERE user_id = ?'
            params = [current_user_id]
        
        if status_filter:
            base_query += ' AND status = ?' if 'WHERE' in base_query else ' WHERE status = ?'
            count_query += ' AND status = ?' if 'WHERE' in count_query else ' WHERE status = ?'
            params.append(status_filter)
        
        # Get total count first
        cursor.execute(count_query, params)
        count_result = cursor.fetchone()
        total = count_result[0] if count_result and count_result[0] is not None else 0
        
        # Build final query with sorting and pagination
        final_query = base_query + f' ORDER BY {sort_by} {sort_order} LIMIT ? OFFSET ?'
        offset = (page - 1) * per_page
        final_params = params + [per_page, offset]
        
        cursor.execute(final_query, final_params)
        videos = cursor.fetchall()
        
        videos_list = []
        for video in videos:
            videos_list.append({
                'id': video[0],
                'filename': video[2],  # original_filename
                'size': video[3] or 0,
                'duration': video[4] or 0,
                'resolution': video[5] or 'Unknown',
                'format': video[6] or 'Unknown',
                'upload_time': video[7],
                'status': video[8] or 'uploaded',
                'user_id': video[9] if current_user_role == 'admin' else None
            })
        
        return jsonify({
            'videos': videos_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': max(1, (total + per_page - 1) // per_page) if total > 0 else 1
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_videos: {str(e)}")
        return jsonify({
            'videos': [],
            'pagination': {'page': 1, 'per_page': per_page, 'total': 0, 'pages': 1},
            'message': 'No videos found'
        }), 200
    finally:
        conn.close()

@app.route('/api/v1/videos/<video_id>/transcode', methods=['POST'])
@token_required
def transcode_video(current_user_id, current_user_role, video_id):
    data = request.get_json()
    target_format = data.get('target_format', 'h264')
    target_resolution = data.get('target_resolution', '720p')
    target_bitrate = data.get('target_bitrate', '2M')
    
    valid_formats = ['h264', 'h265', 'vp9']
    valid_resolutions = ['480p', '720p', '1080p', '4K']
    
    if target_format not in valid_formats:
        return jsonify({'message': 'Invalid target format'}), 400
    
    if target_resolution not in valid_resolutions:
        return jsonify({'message': 'Invalid target resolution'}), 400
    
    conn = get_db()
    cursor = conn.cursor()
    
    # Verify video ownership or admin access
    if current_user_role == 'admin':
        cursor.execute('SELECT filename FROM videos WHERE id = ?', (video_id,))
    else:
        cursor.execute('SELECT filename FROM videos WHERE id = ? AND user_id = ?', (video_id, current_user_id))
    
    video = cursor.fetchone()
    if not video:
        conn.close()
        return jsonify({'message': 'Video not found'}), 404
    
    # Create transcoding job
    job_id = str(uuid.uuid4())
    result_filename = f"{job_id}_{target_format}_{target_resolution}.mp4"
    
    cursor.execute('''
        INSERT INTO transcoding_jobs (id, user_id, video_id, target_format, 
                                    target_resolution, target_bitrate, result_filename)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (job_id, current_user_id, video_id, target_format, target_resolution, 
          target_bitrate, result_filename))
    conn.commit()
    conn.close()
    
    # Start transcoding in background
    input_path = os.path.join(app.config['UPLOAD_FOLDER'], video[0])
    output_path = os.path.join(app.config['TRANSCODED_FOLDER'], result_filename)
    
    executor.submit(transcode_video_cpu_intensive, input_path, output_path, 
                   target_format, target_resolution, target_bitrate, job_id)
    
    return jsonify({
        'message': 'Transcoding started',
        'job_id': job_id,
        'target_format': target_format,
        'target_resolution': target_resolution
    }), 202

@app.route('/api/v1/transcoding/jobs/<job_id>', methods=['GET'])
@token_required
def get_transcoding_job(current_user_id, current_user_role, job_id):
    conn = get_db()
    cursor = conn.cursor()
    
    if current_user_role == 'admin':
        cursor.execute('''
            SELECT id, target_format, target_resolution, status, progress, 
                   result_filename, created_at, completed_at, processing_time, error_message
            FROM transcoding_jobs WHERE id = ?
        ''', (job_id,))
    else:
        cursor.execute('''
            SELECT id, target_format, target_resolution, status, progress, 
                   result_filename, created_at, completed_at, processing_time, error_message
            FROM transcoding_jobs WHERE id = ? AND user_id = ?
        ''', (job_id, current_user_id))
    
    job = cursor.fetchone()
    conn.close()
    
    if not job:
        return jsonify({'message': 'Job not found'}), 404
    
    job_data = {
        'id': job[0],
        'target_format': job[1],
        'target_resolution': job[2],
        'status': job[3],
        'progress': job[4],
        'created_at': job[6],
        'completed_at': job[7],
        'processing_time': job[8],
        'error_message': job[9]
    }
    
    if job[5] and job[3] == 'completed':  # result_filename and completed
        job_data['download_url'] = f'/api/v1/transcoding/jobs/{job_id}/download'
    
    return jsonify(job_data), 200

@app.route('/api/v1/transcoding/jobs/<job_id>/download', methods=['GET'])
@token_required
def download_transcoded_video(current_user_id, current_user_role, job_id):
    conn = get_db()
    cursor = conn.cursor()
    
    if current_user_role == 'admin':
        cursor.execute('''
            SELECT result_filename FROM transcoding_jobs 
            WHERE id = ? AND status = 'completed'
        ''', (job_id,))
    else:
        cursor.execute('''
            SELECT result_filename FROM transcoding_jobs 
            WHERE id = ? AND user_id = ? AND status = 'completed'
        ''', (job_id, current_user_id))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result or not result[0]:
        return jsonify({'message': 'Transcoded video not found'}), 404
    
    file_path = os.path.join(app.config['TRANSCODED_FOLDER'], result[0])
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    
    return jsonify({'message': 'File not found'}), 404

@app.route('/api/v1/transcoding/jobs', methods=['GET'])
@token_required
def get_transcoding_jobs(current_user_id, current_user_role):
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    status_filter = request.args.get('status')
    
    conn = get_db()
    cursor = conn.cursor()
    
    try:
        if current_user_role == 'admin':
            base_query = '''
                SELECT j.id, j.target_format, j.target_resolution, j.status, j.progress, 
                       j.created_at, j.completed_at, j.processing_time, v.original_filename, j.user_id
                FROM transcoding_jobs j
                JOIN videos v ON j.video_id = v.id
            '''
            count_query = '''
                SELECT COUNT(*) FROM transcoding_jobs j
                JOIN videos v ON j.video_id = v.id
            '''
            params = []
        else:
            base_query = '''
                SELECT j.id, j.target_format, j.target_resolution, j.status, j.progress, 
                       j.created_at, j.completed_at, j.processing_time, v.original_filename, j.user_id
                FROM transcoding_jobs j
                JOIN videos v ON j.video_id = v.id
                WHERE j.user_id = ?
            '''
            count_query = '''
                SELECT COUNT(*) FROM transcoding_jobs j
                JOIN videos v ON j.video_id = v.id
                WHERE j.user_id = ?
            '''
            params = [current_user_id]
        
        if status_filter:
            base_query += ' AND j.status = ?' if 'WHERE' in base_query else ' WHERE j.status = ?'
            count_query += ' AND j.status = ?' if 'WHERE' in count_query else ' WHERE j.status = ?'
            params.append(status_filter)
        
        # Get total count
        cursor.execute(count_query, params)
        count_result = cursor.fetchone()
        total = count_result[0] if count_result and count_result[0] is not None else 0
        
        # Get paginated results
        final_query = base_query + ' ORDER BY j.created_at DESC LIMIT ? OFFSET ?'
        offset = (page - 1) * per_page
        final_params = params + [per_page, offset]
        
        cursor.execute(final_query, final_params)
        jobs = cursor.fetchall()
        
        jobs_list = []
        for job in jobs:
            job_data = {
                'id': job[0],
                'target_format': job[1],
                'target_resolution': job[2],
                'status': job[3],
                'progress': job[4] or 0,
                'created_at': job[5],
                'completed_at': job[6],
                'processing_time': job[7],
                'video_filename': job[8]
            }
            
            if current_user_role == 'admin':
                job_data['user_id'] = job[9]
            
            if job[3] == 'completed':
                job_data['download_url'] = f'/api/v1/transcoding/jobs/{job[0]}/download'
            
            jobs_list.append(job_data)
        
        return jsonify({
            'jobs': jobs_list,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'pages': max(1, (total + per_page - 1) // per_page) if total > 0 else 1
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error in get_transcoding_jobs: {str(e)}")
        return jsonify({
            'jobs': [],
            'pagination': {'page': 1, 'per_page': per_page, 'total': 0, 'pages': 1},
            'message': 'No jobs found'
        }), 200
    finally:
        conn.close()

@app.route('/api/v1/users/<int:user_id>/preferences', methods=['GET', 'PUT'])
@token_required
def user_preferences(current_user_id, current_user_role, user_id):
    # Only allow users to access their own preferences or admin access
    if current_user_role != 'admin' and current_user_id != user_id:
        return jsonify({'message': 'Access denied'}), 403
    
    conn = get_db()
    cursor = conn.cursor()
    
    if request.method == 'GET':
        cursor.execute('''
            SELECT default_format, default_resolution, default_quality, 
                   notifications_enabled, auto_delete_originals
            FROM user_preferences WHERE user_id = ?
        ''', (user_id,))
        
        prefs = cursor.fetchone()
        conn.close()
        
        if not prefs:
            return jsonify({'message': 'Preferences not found'}), 404
        
        return jsonify({
            'default_format': prefs[0],
            'default_resolution': prefs[1],
            'default_quality': prefs[2],
            'notifications_enabled': bool(prefs[3]),
            'auto_delete_originals': bool(prefs[4])
        }), 200
    
    elif request.method == 'PUT':
        data = request.get_json()
        
        cursor.execute('''
            UPDATE user_preferences 
            SET default_format = ?, default_resolution = ?, default_quality = ?,
                notifications_enabled = ?, auto_delete_originals = ?
            WHERE user_id = ?
        ''', (
            data.get('default_format', 'mp4'),
            data.get('default_resolution', '720p'),
            data.get('default_quality', 'medium'),
            data.get('notifications_enabled', True),
            data.get('auto_delete_originals', False),
            user_id
        ))
        
        conn.commit()
        conn.close()
        
        return jsonify({'message': 'Preferences updated successfully'}), 200

@app.route('/api/v1/stats', methods=['GET'])
@token_required
def get_user_stats(current_user_id, current_user_role):
    conn = get_db()
    cursor = conn.cursor()
    
    if current_user_role == 'admin':
        # Admin gets system-wide stats
        cursor.execute('SELECT COUNT(*), SUM(file_size) FROM videos')
        video_stats = cursor.fetchone()
        
        cursor.execute('SELECT status, COUNT(*) FROM transcoding_jobs GROUP BY status')
        job_stats = dict(cursor.fetchall())
        
        cursor.execute('SELECT AVG(processing_time), SUM(processing_time) FROM transcoding_jobs WHERE status = "completed"')
        time_stats = cursor.fetchone()
        
        cursor.execute('SELECT COUNT(DISTINCT user_id) FROM videos')
        user_count = cursor.fetchone()[0]
        
    else:
        # Regular users get personal stats
        cursor.execute('SELECT COUNT(*), SUM(file_size) FROM videos WHERE user_id = ?', (current_user_id,))
        video_stats = cursor.fetchone()
        
        cursor.execute('SELECT status, COUNT(*) FROM transcoding_jobs WHERE user_id = ? GROUP BY status', (current_user_id,))
        job_stats = dict(cursor.fetchall())
        
        cursor.execute('SELECT AVG(processing_time), SUM(processing_time) FROM transcoding_jobs WHERE user_id = ? AND status = "completed"', (current_user_id,))
        time_stats = cursor.fetchone()
        
        user_count = 1
    
    conn.close()
    
    stats = {
        'videos': {
            'total': video_stats[0] or 0,
            'total_size': video_stats[1] or 0
        },
        'jobs': {
            'total': sum(job_stats.values()),
            'by_status': job_stats
        },
        'processing': {
            'average_time': time_stats[0] or 0,
            'total_time': time_stats[1] or 0
        }
    }
    
    if current_user_role == 'admin':
        stats['system'] = {
            'total_users': user_count
        }
    
    return jsonify(stats), 200

@app.route('/api/v1/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'version': '1.0.0',
        'services': {
            'database': 'connected',
            'ffmpeg': 'available' if check_ffmpeg_available() else 'not_available'
        }
    }), 200

# CPU Load Testing Endpoint
@app.route('/api/v1/stress-test', methods=['POST'])
@token_required
def stress_test(current_user_id, current_user_role):
    """Endpoint to generate CPU load for testing purposes"""
    data = request.get_json() or {}
    duration = data.get('duration', 300)  # Default 5 minutes
    intensity = data.get('intensity', 4)  # Number of concurrent tasks
    
    def cpu_intensive_task():
        """Perform CPU-intensive video processing operations"""
        end_time = time.time() + duration
        while time.time() < end_time:
            # Simulate video processing calculations
            for i in range(50000):
                # Matrix operations similar to video filters
                import math
                result = sum(math.sin(j) * math.cos(j) for j in range(1000))
                result = result * math.sqrt(i + 1)
                
                # Simulate colorspace conversions
                rgb_value = (i % 256, (i * 2) % 256, (i * 3) % 256)
                yuv_y = 0.299 * rgb_value[0] + 0.587 * rgb_value[1] + 0.114 * rgb_value[2]
                yuv_u = -0.169 * rgb_value[0] - 0.331 * rgb_value[1] + 0.5 * rgb_value[2]
                yuv_v = 0.5 * rgb_value[0] - 0.419 * rgb_value[1] - 0.081 * rgb_value[2]
            
            time.sleep(0.001)  # Small pause to prevent complete system lock
    
    # Start multiple CPU-intensive tasks
    for _ in range(intensity):
        executor.submit(cpu_intensive_task)
    
    return jsonify({
        'message': f'Started {intensity} CPU-intensive video processing tasks for {duration} seconds',
        'duration': duration,
        'intensity': intensity
    }), 200

# Batch transcoding endpoint for high load generation
@app.route('/api/v1/transcoding/batch', methods=['POST'])
@token_required
def batch_transcode(current_user_id, current_user_role):
    """Process multiple videos simultaneously for load testing"""
    data = request.get_json()
    video_ids = data.get('video_ids', [])
    target_format = data.get('target_format', 'h264')
    target_resolution = data.get('target_resolution', '720p')
    target_bitrate = data.get('target_bitrate', '2M')
    
    if not video_ids:
        return jsonify({'message': 'No video IDs provided'}), 400
    
    job_ids = []
    
    for video_id in video_ids:
        # Create individual transcoding job
        job_id = str(uuid.uuid4())
        result_filename = f"{job_id}_{target_format}_{target_resolution}.mp4"
        
        conn = get_db()
        cursor = conn.cursor()
        
        # Verify video exists and user has access
        if current_user_role == 'admin':
            cursor.execute('SELECT filename FROM videos WHERE id = ?', (video_id,))
        else:
            cursor.execute('SELECT filename FROM videos WHERE id = ? AND user_id = ?', (video_id, current_user_id))
        
        video = cursor.fetchone()
        if video:
            cursor.execute('''
                INSERT INTO transcoding_jobs (id, user_id, video_id, target_format, 
                                            target_resolution, target_bitrate, result_filename)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (job_id, current_user_id, video_id, target_format, target_resolution, 
                  target_bitrate, result_filename))
            
            # Start transcoding in background
            input_path = os.path.join(app.config['UPLOAD_FOLDER'], video[0])
            output_path = os.path.join(app.config['TRANSCODED_FOLDER'], result_filename)
            
            executor.submit(transcode_video_cpu_intensive, input_path, output_path, 
                           target_format, target_resolution, target_bitrate, job_id)
            
            job_ids.append(job_id)
        
        conn.commit()
        conn.close()
    
    return jsonify({
        'message': f'Batch transcoding started for {len(job_ids)} videos',
        'job_ids': job_ids,
        'total_jobs': len(job_ids)
    }), 202

@app.route('/api/v1/videos/<video_id>/download', methods=['GET'])
@token_required
def download_original_video(current_user_id, current_user_role, video_id):
    """Download original video file"""
    conn = get_db()
    cursor = conn.cursor()
    
    if current_user_role == 'admin':
        cursor.execute('SELECT filename, original_filename FROM videos WHERE id = ?', (video_id,))
    else:
        cursor.execute('SELECT filename, original_filename FROM videos WHERE id = ? AND user_id = ?', (video_id, current_user_id))
    
    video = cursor.fetchone()
    conn.close()
    
    if not video:
        return jsonify({'message': 'Video not found'}), 404
    
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], video[0])
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True, download_name=video[1])
    
    return jsonify({'message': 'File not found'}), 404

@app.route('/api/v1/videos/<video_id>', methods=['DELETE'])
@token_required
def delete_video(current_user_id, current_user_role, video_id):
    """Delete video and associated transcoding jobs"""
    conn = get_db()
    cursor = conn.cursor()
    
    # Verify video ownership or admin access
    if current_user_role == 'admin':
        cursor.execute('SELECT filename FROM videos WHERE id = ?', (video_id,))
    else:
        cursor.execute('SELECT filename FROM videos WHERE id = ? AND user_id = ?', (video_id, current_user_id))
    
    video = cursor.fetchone()
    if not video:
        conn.close()
        return jsonify({'message': 'Video not found'}), 404
    
    # Delete original file
    original_path = os.path.join(app.config['UPLOAD_FOLDER'], video[0])
    if os.path.exists(original_path):
        os.remove(original_path)
    
    # Delete transcoded files
    cursor.execute('SELECT result_filename FROM transcoding_jobs WHERE video_id = ?', (video_id,))
    transcoded_files = cursor.fetchall()
    
    for file_tuple in transcoded_files:
        if file_tuple[0]:
            transcoded_path = os.path.join(app.config['TRANSCODED_FOLDER'], file_tuple[0])
            if os.path.exists(transcoded_path):
                os.remove(transcoded_path)
    
    # Delete database records
    cursor.execute('DELETE FROM transcoding_jobs WHERE video_id = ?', (video_id,))
    cursor.execute('DELETE FROM videos WHERE id = ?', (video_id,))
    
    conn.commit()
    conn.close()
    
    return jsonify({'message': 'Video deleted successfully'}), 200

if __name__ == '__main__':
    # Create directories if they don't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['TRANSCODED_FOLDER'], exist_ok=True)
    
    # Check FFmpeg availability
    if not check_ffmpeg_available():
        logger.warning("FFmpeg not found. Video transcoding will not work. Install with: sudo apt install ffmpeg")
    
    app.run(host='0.0.0.0', port=5000, debug=True)