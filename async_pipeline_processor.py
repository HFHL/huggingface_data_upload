#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import shutil
import tarfile
import asyncio
import pandas as pd
import soundfile as sf
from pathlib import Path
from datetime import datetime
from datasets import Dataset, Features, Value, Audio, Sequence
from huggingface_hub import HfApi, create_repo, CommitOperationAdd
import logging
import gc
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor
import threading
import concurrent.futures
import argparse

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class AsyncPipelineProcessor:
    def __init__(self, base_dir="./downloads", tar_batch_size=2, check_interval=5, parquet_batch_size=20, repo_id=None, hf_token=None):
        self.base_dir = Path(base_dir)
        self.tar_batch_size = tar_batch_size  # 每次处理2个tar文件
        self.check_interval = check_interval  # 缩短检查间隔到5秒
        
        # 目录设置
        self.processed_dir = self.base_dir / "processed"
        self.unzipped_dir = self.base_dir / "unzipped"
        self.parquet_dir = self.base_dir / "parquet"
        self.corrupted_dir = self.base_dir / "corrupted"
        
        # 创建必要的目录
        for dir_path in [self.processed_dir, self.unzipped_dir, 
                        self.parquet_dir, self.corrupted_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # 记录文件
        self.unzip_record = self.processed_dir / "unzip_record.txt"
        self.parquet_record = self.processed_dir / "parquet_record.txt"
        self.upload_record = self.processed_dir / "upload_record.txt"
        self.corrupted_record = self.processed_dir / "corrupted_record.txt"
        
        # 初始化HuggingFace API
        self.hf_token = hf_token or os.environ.get("HF_TOKEN")
        if not self.hf_token:
            raise ValueError("必须提供HuggingFace token，可以通过参数传入或设置环境变量HF_TOKEN")
            
        self.hf_api = HfApi(token=self.hf_token)
        self.repo_id = repo_id
        if not self.repo_id:
            raise ValueError("必须提供目标仓库ID")
        
        # 线程池 - 增加线程数以支持并行处理
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # 停止标志
        self.stop_event = Event()
        
        # 处理队列
        self.tar_queue = Queue()
        self.parquet_queue = Queue()
        
        # 处理线程
        self.scan_thread = None
        self.tar_thread = None
        self.upload_thread = None
        
        # 当前正在处理的tar文件计数
        self.processing_count = 0
        self.processing_lock = threading.Lock()
        
        # 添加性能统计
        self.stats = {
            'start_time': None,
            'processed_tars': 0,
            'processed_audios': 0,
            'uploaded_files': 0,
            'failed_tars': 0,
            'failed_audios': 0,
            'failed_uploads': 0,
            'total_audio_duration': 0,
            'total_processing_time': 0
        }
        
        # 为每个处理阶段创建单独的日志记录器
        self.tar_logger = logging.getLogger('tar_processor')
        self.audio_logger = logging.getLogger('audio_processor')
        self.upload_logger = logging.getLogger('upload_processor')
        
        # 设置日志格式和文件
        self._setup_loggers()
        
        # 添加parquet处理的批次大小
        self.parquet_batch_size = parquet_batch_size
        
        # 添加音频处理队列和线程
        self.audio_queue = Queue()
        self.audio_thread = None
        
        # 添加音频处理的线程池
        self.audio_executor = ThreadPoolExecutor(max_workers=4)
        
        # 添加音频处理记录
        self.audio_record = self.processed_dir / "audio_record.txt"
        
        # 添加上传批次目录
        self.upload_batch_dir = self.base_dir / "upload_batch"
        self.upload_batch_dir.mkdir(exist_ok=True)
        
        # 修改为每批次10个文件
        self.upload_batch_size = 10
        
        # 保留重试相关设置
        self.max_retries = 3
        self.retry_delay = 60
        
        # 添加上传锁
        self.upload_lock = threading.Lock()
    
    def _setup_loggers(self):
        """设置各个处理阶段的日志记录器"""
        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 创建文件处理器
        tar_handler = logging.FileHandler('tar_processing.log', encoding='utf-8')
        audio_handler = logging.FileHandler('audio_processing.log', encoding='utf-8')
        upload_handler = logging.FileHandler('upload_processing.log', encoding='utf-8')
        
        # 设置格式化器
        tar_handler.setFormatter(formatter)
        audio_handler.setFormatter(formatter)
        upload_handler.setFormatter(formatter)
        
        # 添加处理器到日志记录器
        self.tar_logger.addHandler(tar_handler)
        self.audio_logger.addHandler(audio_handler)
        self.upload_logger.addHandler(upload_handler)
        
        # 设置日志级别
        self.tar_logger.setLevel(logging.INFO)
        self.audio_logger.setLevel(logging.INFO)
        self.upload_logger.setLevel(logging.INFO)
    
    def _log_stats(self):
        """记录处理统计信息"""
        if self.stats['start_time']:
            elapsed_time = time.time() - self.stats['start_time']
            logger.info(f"""
处理统计信息:
运行时间: {elapsed_time:.2f}秒
处理的tar文件: {self.stats['processed_tars']}
处理的音频文件: {self.stats['processed_audios']}
上传的文件: {self.stats['uploaded_files']}
失败的tar文件: {self.stats['failed_tars']}
失败的音频文件: {self.stats['failed_audios']}
失败的上传: {self.stats['failed_uploads']}
总音频时长: {self.stats['total_audio_duration']:.2f}秒
平均处理时间: {self.stats['total_processing_time']/max(1, self.stats['processed_tars']):.2f}秒/文件
""")
    
    def load_record(self, record_file):
        """加载处理记录"""
        if record_file.exists():
            return set(record_file.read_text().splitlines())
        return set()
    
    def save_record(self, record_file, item):
        """保存处理记录"""
        with open(record_file, 'a') as f:
            f.write(f"{item}\n")
    
    def scan_tar_files(self):
        """扫描tar文件并加入队列"""
        while not self.stop_event.is_set():
            try:
                processed_tars = self.load_record(self.unzip_record)
                logger.info(f"已处理的tar文件数量: {len(processed_tars)}")
                
                # 如果当前处理的文件数量小于批次大小，继续添加文件
                with self.processing_lock:
                    if self.processing_count < self.tar_batch_size:
                        logger.info(f"当前处理数量: {self.processing_count}, 批次大小: {self.tar_batch_size}")
                        # 检查基础目录是否存在
                        if not self.base_dir.exists():
                            logger.error(f"基础目录不存在: {self.base_dir}")
                            time.sleep(self.check_interval)
                            continue
                            
                        # 列出所有匹配的目录
                        page_batch_dirs = list(self.base_dir.glob("page_*_batch_*"))
                        logger.info(f"找到 {len(page_batch_dirs)} 个批次目录")
                        
                        for page_batch_dir in page_batch_dirs:
                            logger.info(f"扫描目录: {page_batch_dir}")
                            if page_batch_dir.is_dir():
                                # 列出所有tar文件
                                tar_files = list(page_batch_dir.glob("*.tar"))
                                logger.info(f"在 {page_batch_dir} 中找到 {len(tar_files)} 个tar文件")
                                
                                for tar_file in tar_files:
                                    try:
                                        # 检查文件权限和大小
                                        if not tar_file.exists():
                                            logger.error(f"tar文件不存在: {tar_file}")
                                            continue
                                            
                                        file_size = tar_file.stat().st_size
                                        logger.info(f"检查tar文件: {tar_file} (大小: {self._format_size(file_size)})")
                                        
                                        if str(tar_file) not in processed_tars:
                                            self.tar_queue.put(tar_file)
                                            logger.info(f"添加新的tar文件到队列: {tar_file}")
                                            self.processing_count += 1
                                            
                                            # 如果达到批次大小，停止添加
                                            if self.processing_count >= self.tar_batch_size:
                                                break
                                    except Exception as e:
                                        logger.error(f"处理tar文件时出错 {tar_file}: {str(e)}")
                                        
                            if self.processing_count >= self.tar_batch_size:
                                break
                    else:
                        logger.info("当前处理数量已达到批次大小限制")
                
            except Exception as e:
                logger.error(f"扫描tar文件时出错: {str(e)}")
                
            time.sleep(self.check_interval)
    
    def verify_tar_integrity(self, tar_path):
        """验证tar文件的完整性"""
        try:
            if not tar_path.exists():
                return False, "文件不存在"
            
            if tar_path.stat().st_size == 0:
                return False, "文件大小为0"
            
            with tarfile.open(tar_path, 'r') as tar:
                try:
                    members = tar.getmembers()
                    if not members:
                        return False, "tar文件为空"
                    
                    has_flac = has_json = False
                    for member in members:
                        if member.name.endswith('.flac'):
                            has_flac = True
                        elif member.name.endswith('.json'):
                            has_json = True
                        if has_flac and has_json:
                            break
                    
                    if not (has_flac and has_json):
                        return False, "tar文件缺少必要的文件类型"
                    
                    return True, "文件完整"
                    
                except Exception as e:
                    return False, f"tar文件结构验证失败: {str(e)}"
                    
        except Exception as e:
            return False, f"验证过程出错: {str(e)}"
    
    def process_single_tar(self, tar_path):
        """处理单个tar文件，解压后将音频文件加入处理队列"""
        start_time = time.time()
        self.tar_logger.info(f"开始处理tar文件: {tar_path}")
        
        try:
            # 验证文件完整性
            is_valid, error_msg = self.verify_tar_integrity(tar_path)
            if not is_valid:
                self.tar_logger.error(f"tar文件验证失败: {tar_path} - {error_msg}")
                self.handle_corrupted_tar(tar_path, error_msg)
                self.stats['failed_tars'] += 1
                return
            
            # 记录文件大小
            file_size = tar_path.stat().st_size
            self.tar_logger.info(f"tar文件大小: {self._format_size(file_size)}")
            
            # 解压文件
            extract_dir = self.unzipped_dir / tar_path.stem
            self.tar_logger.info(f"开始解压到: {extract_dir}")
            
            with tarfile.open(tar_path, 'r') as tar:
                members = tar.getmembers()
                self.tar_logger.info(f"tar文件包含 {len(members)} 个文件")
                tar.extractall(path=extract_dir)
            
            # 将解压目录加入音频处理队列
            self.audio_queue.put((extract_dir, tar_path))
            
            # 记录处理完成并删除tar文件
            self.save_record(self.unzip_record, str(tar_path))
            tar_path.unlink()
            
            # 减少处理计数
            with self.processing_lock:
                self.processing_count -= 1
            
        except Exception as e:
            self.tar_logger.error(f"处理tar文件失败: {tar_path}", exc_info=True)
            self.stats['failed_tars'] += 1
            with self.processing_lock:
                self.processing_count -= 1
    
    def process_tar_files(self):
        """处理tar文件的线程"""
        while not self.stop_event.is_set():
            try:
                # 非阻塞方式获取任务
                try:
                    tar_path = self.tar_queue.get_nowait()
                    logger.info(f"从队列获取到tar文件: {tar_path}")
                except Empty:
                    logger.debug("tar队列为空，等待新文件...")
                    time.sleep(self.check_interval)
                    continue
                
                # 使用线程池处理tar文件
                logger.info(f"提交tar文件到处理线程池: {tar_path}")
                self.executor.submit(self.process_single_tar, tar_path)
                
            except Exception as e:
                logger.error(f"tar处理线程出错: {e}")
                time.sleep(1)
    
    def process_audio_batch(self, audio_files, extract_dir):
        """并行处理一批音频文件"""
        records = []
        total_audio_duration = 0
        
        def process_single_audio(audio_file):
            try:
                self.audio_logger.info(f"处理音频文件: {audio_file}")
                
                # 读取音频
                audio_data, sample_rate = sf.read(str(audio_file))
                audio_len = len(audio_data) / sample_rate
                
                self.audio_logger.info(
                    f"音频信息 - 长度: {audio_len:.2f}秒, "
                    f"采样率: {sample_rate}Hz, "
                    f"大小: {self._format_size(os.path.getsize(audio_file))}"
                )
                
                # 读取JSON
                json_path = audio_file.with_suffix('.json')
                metadata = {}
                if json_path.exists():
                    with open(json_path, 'r', encoding='utf-8') as f:
                        metadata = json.load(f)
                
                # 创建记录
                record = {
                    'index': str(audio_file.relative_to(extract_dir)),
                    'datasetname': 'a_t5',
                    'audio': {
                        'array': audio_data,
                        'sampling_rate': sample_rate
                    },
                    'audio_len': audio_len,
                    'text': metadata.get('text', ''),
                    'raw_text': self._extract_raw_text(metadata)
                }
                
                # 删除原始文件
                audio_file.unlink()
                if json_path.exists():
                    json_path.unlink()
                
                self.audio_logger.info(f"已处理并删除: {audio_file}")
                return record, audio_len
                
            except Exception as e:
                self.audio_logger.error(f"处理音频文件失败: {audio_file} - {str(e)}", exc_info=True)
                self.stats['failed_audios'] += 1
                return None, 0
        
        # 使用线程池并行处理音频文件
        futures = []
        for audio_file in audio_files:
            if str(audio_file) not in self.load_record(self.audio_record):
                futures.append(self.audio_executor.submit(process_single_audio, audio_file))
        
        # 收集处理结果
        for future in concurrent.futures.as_completed(futures):
            record, audio_len = future.result()
            if record:
                records.append(record)
                total_audio_duration += audio_len
                self.stats['processed_audios'] += 1
        
        return records, total_audio_duration

    def process_audio_files(self):
        """处理音频文件的线程"""
        while not self.stop_event.is_set():
            try:
                try:
                    extract_dir, tar_path = self.audio_queue.get_nowait()
                    logger.info(f"从队列获取到音频目录: {extract_dir}")
                except Empty:
                    logger.debug("音频队列为空，等待新文件...")
                    time.sleep(self.check_interval)
                    continue

                start_time = time.time()
                audio_files = list(extract_dir.rglob("*.flac"))
                total_files = len(audio_files)
                logger.info(f"在目录 {extract_dir} 中找到 {total_files} 个音频文件")
                processed_files = 0

                # 按批次处理音频文件
                for i in range(0, len(audio_files), self.parquet_batch_size):
                    batch = audio_files[i:i + self.parquet_batch_size]
                    logger.info(f"开始处理第 {i//self.parquet_batch_size + 1} 批音频文件，共 {len(batch)} 个文件")
                    records, audio_duration = self.process_audio_batch(batch, extract_dir)

                    if records:
                        # 生成parquet文件名
                        batch_num = i // self.parquet_batch_size
                        parquet_path = self.parquet_dir / f"{tar_path.stem}_batch{batch_num}.parquet"

                        # 保存parquet文件
                        logger.info(
                            f"生成parquet文件: {parquet_path} "
                            f"(包含 {len(records)} 条记录)"
                        )
                        self._save_to_parquet(records, parquet_path)

                        # 将文件添加到待上传列表
                        logger.info(f"添加parquet文件到上传队列: {parquet_path}")
                        self.parquet_queue.put(parquet_path)

                        # 更新统计信息
                        self.stats['total_audio_duration'] += audio_duration
                        processed_files += len(records)

                # 删除解压目录
                shutil.rmtree(extract_dir)
                logger.info(f"清理解压目录: {extract_dir}")

                # 记录处理时间
                processing_time = time.time() - start_time
                self.stats['total_processing_time'] += processing_time

                logger.info(
                    f"完成处理音频目录: {extract_dir} - "
                    f"处理了 {processed_files}/{total_files} 个音频文件, "
                    f"耗时: {processing_time:.2f}秒"
                )

            except Exception as e:
                logger.error(f"处理音频目录失败: {extract_dir}", exc_info=True)
                time.sleep(1)

    def upload_batch_files(self):
        """批量上传文件的线程"""
        while not self.stop_event.is_set():
            try:
                # 收集要上传的文件
                files_to_upload = []
                start_collect_time = time.time()
                
                while len(files_to_upload) < self.upload_batch_size:
                    try:
                        # 检查队列大小
                        queue_size = self.parquet_queue.qsize()
                        logger.info(f"当前上传队列大小: {queue_size}")
                        
                        # 如果队列为空，等待一段时间
                        if queue_size == 0:
                            time_waiting = time.time() - start_collect_time
                            # 如果已经收集到文件且等待超过30秒，开始上传
                            if files_to_upload and time_waiting > 30:
                                logger.info(f"等待超时 ({time_waiting:.2f}秒)，开始上传当前批次")
                                break
                            time.sleep(self.check_interval)
                            continue
                        
                        parquet_file = self.parquet_queue.get_nowait()
                        logger.info(f"从上传队列获取到文件: {parquet_file}")
                        
                        # 验证文件存在且未上传
                        if not parquet_file.exists():
                            logger.error(f"文件不存在: {parquet_file}")
                            continue
                            
                        if str(parquet_file) not in self.load_record(self.upload_record):
                            files_to_upload.append(parquet_file)
                            logger.info(f"添加文件到上传批次: {parquet_file}")
                            
                            # 如果达到批次大小，立即开始上传
                            if len(files_to_upload) >= self.upload_batch_size:
                                logger.info("达到批次大小，开始上传")
                                break
                        else:
                            logger.info(f"文件已上传过，跳过: {parquet_file}")
                            
                    except Empty:
                        # 如果已经收集到文件且等待超过30秒，开始上传
                        time_waiting = time.time() - start_collect_time
                        if files_to_upload and time_waiting > 30:
                            logger.info(f"等待超时 ({time_waiting:.2f}秒)，开始上传当前批次")
                            break
                        time.sleep(self.check_interval)
                        continue

                # 如果没有文件要上传，继续等待
                if not files_to_upload:
                    time.sleep(self.check_interval)
                    continue

                logger.info(f"准备上传批次，共 {len(files_to_upload)} 个文件")

                # 清空并重建上传批次目录
                if self.upload_batch_dir.exists():
                    shutil.rmtree(self.upload_batch_dir)
                self.upload_batch_dir.mkdir(exist_ok=True)

                # 复制文件到上传批次目录
                for file in files_to_upload:
                    try:
                        shutil.copy2(file, self.upload_batch_dir / file.name)
                        logger.info(f"复制文件到上传目录: {file}")
                    except Exception as e:
                        logger.error(f"复制文件失败: {file} - {str(e)}")
                        files_to_upload.remove(file)

                if not files_to_upload:
                    logger.error("所有文件复制失败，跳过此批次")
                    continue

                # 上传整个文件夹
                retries = 0
                while retries < self.max_retries:
                    try:
                        start_time = time.time()
                        total_size = sum(f.stat().st_size for f in self.upload_batch_dir.glob("*.parquet"))

                        logger.info(
                            f"开始上传批次 (尝试 {retries + 1}/{self.max_retries}): "
                            f"{len(files_to_upload)} 个文件, "
                            f"总大小: {self._format_size(total_size)}"
                        )

                        # 确保仓库存在
                        try:
                            self.hf_api.repo_info(repo_id=self.repo_id)
                            logger.info(f"仓库 {self.repo_id} 已存在")
                        except Exception:
                            logger.info(f"创建仓库: {self.repo_id}")
                            create_repo(
                                repo_id=self.repo_id,
                                token=self.hf_token,
                                exist_ok=True,
                                repo_type="dataset"
                            )

                        # 上传文件夹
                        logger.info("开始上传文件夹...")
                        self.hf_api.upload_folder(
                            folder_path=str(self.upload_batch_dir),
                            path_in_repo="data/train",
                            repo_id=self.repo_id,
                            repo_type="dataset"
                        )
                        logger.info("文件夹上传完成")

                        # 记录上传完成的文件并删除原始文件
                        for file in files_to_upload:
                            try:
                                self.save_record(self.upload_record, str(file))
                                file.unlink()  # 删除原始文件
                                logger.info(f"文件上传成功并删除: {file}")
                            except Exception as e:
                                logger.error(f"处理已上传文件时出错: {file} - {str(e)}")

                        upload_time = time.time() - start_time
                        upload_speed = total_size / upload_time if upload_time > 0 else 0

                        with self.upload_lock:
                            self.stats['uploaded_files'] += len(files_to_upload)

                        logger.info(
                            f"批次上传完成 - "
                            f"上传了 {len(files_to_upload)} 个文件, "
                            f"耗时: {upload_time:.2f}秒, "
                            f"速度: {self._format_speed(upload_speed)}"
                        )
                        break

                    except Exception as e:
                        if "429 Client Error" in str(e):
                            logger.warning(f"触发速率限制，等待 {self.retry_delay} 秒后重试...")
                            time.sleep(self.retry_delay)
                            retries += 1
                        else:
                            logger.error(f"上传失败: {str(e)}", exc_info=True)
                            with self.upload_lock:
                                self.stats['failed_uploads'] += len(files_to_upload)
                            break

                # 清理上传批次目录
                try:
                    shutil.rmtree(self.upload_batch_dir)
                    self.upload_batch_dir.mkdir(exist_ok=True)
                    logger.info("清理上传批次目录完成")
                except Exception as e:
                    logger.error(f"清理上传批次目录失败: {str(e)}")

            except Exception as e:
                logger.error(f"批量上传过程出错: {str(e)}", exc_info=True)
                time.sleep(self.check_interval)

    def _extract_raw_text(self, metadata):
        """从元数据中提取raw_text"""
        raw_text = []
        
        if 'original_data' in metadata:
            original_data = metadata['original_data']
            if 'class_names' in original_data:
                raw_text.extend(original_data['class_names'])
            if 'class_labels' in original_data:
                raw_text.extend(original_data['class_labels'])
        
        if 'tag' in metadata and isinstance(metadata['tag'], list):
            raw_text.extend(metadata['tag'])
        
        if 'text_augment_t5' in metadata:
            raw_text.append(metadata['text_augment_t5'])
        
        if 'text_augment_all' in metadata and isinstance(metadata['text_augment_all'], list):
            raw_text.extend(metadata['text_augment_all'])
        
        return raw_text
    
    def _save_to_parquet(self, records, output_path):
        """保存记录为parquet文件"""
        df = pd.DataFrame(records)
        features = Features({
            'index': Value('string'),
            'datasetname': Value('string'),
            'audio': Audio(),
            'audio_len': Value('float32'),
            'text': Value('string'),
            'raw_text': Sequence(Value('string'))
        })
        
        dataset = Dataset.from_pandas(df, features=features)
        dataset.to_parquet(str(output_path))
    
    def start(self):
        """启动所有处理线程"""
        self.stats['start_time'] = time.time()
        logger.info("启动处理流水线...")
        
        # 加载现有的 parquet 文件到上传队列
        logger.info("检查现有的 parquet 文件...")
        existing_parquet_files = list(self.parquet_dir.glob("*.parquet"))
        uploaded_files = self.load_record(self.upload_record)
        for parquet_file in existing_parquet_files:
            if str(parquet_file) not in uploaded_files:
                logger.info(f"添加现有 parquet 文件到上传队列: {parquet_file}")
                self.parquet_queue.put(parquet_file)
        logger.info(f"已添加 {len(existing_parquet_files)} 个现有 parquet 文件到上传队列")
        
        # 启动文件扫描线程
        self.scan_thread = Thread(target=self.scan_tar_files)
        self.scan_thread.daemon = True
        self.scan_thread.start()
        
        # 启动tar处理线程
        self.tar_thread = Thread(target=self.process_tar_files)
        self.tar_thread.daemon = True
        self.tar_thread.start()
        
        # 启动音频处理线程
        self.audio_thread = Thread(target=self.process_audio_files)
        self.audio_thread.daemon = True
        self.audio_thread.start()
        
        # 启动批量上传线程
        self.upload_thread = Thread(target=self.upload_batch_files)
        self.upload_thread.daemon = True
        self.upload_thread.start()
        
        logger.info("所有处理线程已启动")
    
    def stop(self):
        """停止所有处理线程"""
        logger.info("正在停止处理流水线...")
        self._log_stats()  # 记录最终统计信息
        self.stop_event.set()

        # 等待所有线程结束
        for thread in [self.scan_thread, self.tar_thread, 
                      self.audio_thread, self.upload_thread]:
            if thread and thread.is_alive():
                thread.join()

        # 关闭线程池
        self.executor.shutdown()
        self.audio_executor.shutdown()
        logger.info("处理流水线已停止")

    def _format_size(self, size_bytes):
        """格式化文件大小"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

    def _format_speed(self, bytes_per_second):
        """格式化速度"""
        return f"{self._format_size(bytes_per_second)}/s"

if __name__ == "__main__":
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description='异步数据处理流水线')
    parser.add_argument('--page', type=int, required=True, help='页码，例如：3')
    parser.add_argument('--batch', type=int, required=True, help='批次号，例如：2')
    parser.add_argument('--tar-batch-size', type=int, default=2, help='同时处理的tar文件数量（默认：2）')
    parser.add_argument('--check-interval', type=int, default=5, help='检查新文件的间隔，单位秒（默认：5）')
    parser.add_argument('--parquet-batch-size', type=int, default=20, help='每个parquet文件包含的记录数（默认：20）')
    parser.add_argument('--token', type=str, help='HuggingFace token，如果不提供则从环境变量HF_TOKEN获取')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 构建仓库ID
    repo_id = f'CLAPv2/a_t5_page{args.page}_batch{args.batch}'
    
    # 获取token
    hf_token = args.token or os.environ.get('HF_TOKEN')
    if not hf_token:
        parser.error("必须提供 --token 参数或设置 HF_TOKEN 环境变量")
    
    # 配置参数
    config = {
        'repo_id': repo_id,
        'hf_token': hf_token,
        'tar_batch_size': args.tar_batch_size,
        'check_interval': args.check_interval,
        'parquet_batch_size': args.parquet_batch_size
    }
    
    # 打印配置信息
    logger.info(f"启动配置:")
    logger.info(f"仓库ID: {config['repo_id']}")
    logger.info(f"tar批次大小: {config['tar_batch_size']}")
    logger.info(f"检查间隔: {config['check_interval']}秒")
    logger.info(f"parquet批次大小: {config['parquet_batch_size']}")
    
    # 创建处理器实例
    processor = AsyncPipelineProcessor(
        tar_batch_size=config['tar_batch_size'],
        check_interval=config['check_interval'],
        parquet_batch_size=config['parquet_batch_size'],
        repo_id=config['repo_id'],
        hf_token=config['hf_token']
    )
    
    try:
        processor.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("接收到停止信号，正在关闭...")
        processor.stop() 