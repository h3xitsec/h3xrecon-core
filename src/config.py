"""
Configuration management for H3XRecon.
Handles loading configuration from config files.
"""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from loguru import logger

@dataclass
class DatabaseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    min_size: int = 10
    max_size: int = 20

    def to_dict(self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'min_size': self.min_size,
            'max_size': self.max_size,
        }

@dataclass
class NatsConfig:
    host: str
    port: int
    user: Optional[str] = None
    password: Optional[str] = None
    
    @property
    def url(self) -> str:
        if self.user and self.password:
            return f"nats://{self.user}:{self.password}@{self.host}:{self.port}"
        return f"nats://{self.host}:{self.port}"

@dataclass
class RedisConfig:
    host: str
    port: int
    db: int = 0
    password: Optional[str] = None

@dataclass
class LogConfig:
    level: str
    format: str
    file_path: Optional[str] = None

class Config:
    """
    Central configuration management for H3XRecon.
    Loads configuration from config file.
    """
    
    def __init__(self):
        """Initialize configuration from file or environment variables."""
        #load_dotenv()  # Load environment variables from .env file if it exists
        #if os.getenv('H3XRECON_CONFIG'):
        #    self._load_config(os.getenv('H3XRECON_CONFIG'))
        #else:
        self._load_from_env()

    # def _load_config(self, config_path: str):
    #     """Load configuration from a JSON file."""
    #     try:
    #         config_file = Path(config_path)

    #         if not config_file.exists():
    #             logger.warning(f"Config file {config_path} not found, falling back to environment variables")
    #             return self._load_from_env()
            
    #         with open(config_file, 'r') as f:
    #             self.config_data = json.load(f)
    #         # Load individual configurations from file with defaults
    #         database_config = {
    #             'host': 'localhost',
    #             'port': 5432,
    #             'database': 'h3xrecon',
    #             'user': 'postgres',
    #             'password': '',
    #             'min_size': 10,
    #             'max_size': 20,
    #             **self.config_data.get('database', {})
    #         }
    #         print(database_config)
    #         nats_config = {
    #             'host': 'localhost',
    #             'port': 4222,
    #             **self.config_data.get('nats', {})
    #         }
            
    #         redis_config = {
    #             'host': 'localhost',
    #             'port': 6379,
    #             'db': 0,
    #             **self.config_data.get('redis', {})
    #         }
            
    #         log_config = {
    #             'level': 'INFO',
    #             'format': '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>',
    #             **self.config_data.get('logging', {})
    #         }
            
    #         # Create configuration objects with merged defaults
    #         self.database = DatabaseConfig(**database_config)
    #         self.nats = NatsConfig(**nats_config)
    #         self.redis = RedisConfig(**redis_config)
    #         self.logging = LogConfig(**log_config)
            
    #         # Additional configurations
    #         self.worker_execution_threshold = self.config_data.get('worker_execution_threshold', 24)
    #         self.debug_mode = self.config_data.get('debug_mode', False)
            
    #     except Exception as e:
    #         logger.error(f"Error loading configuration from file {config_path}: {e}")
    #         logger.info("Falling back to environment variables")
    #         self._load_from_env()

    def _load_from_env(self):
        """Load all configurations from environment variables."""
        try:
            # Initialize config_data as empty dict since we're not loading from file
            self.config_data = {}
            
            # Load individual configurations from environment
            self.database = self._load_database_config_env()
            self.nats = self._load_nats_config_env()
            self.redis = self._load_redis_config_env()
            self.logging = self._load_log_config_env()
            
            # Additional configurations
            self.worker_execution_threshold = int(os.getenv('H3XRECON_WORKER_THRESHOLD', '24'))
            self.debug_mode = os.getenv('H3XRECON_DEBUG', 'false').lower() == 'true'
        
        except Exception as e:
            logger.error(f"Error loading configuration from environment: {e}")
            raise

    def _load_database_config_env(self) -> DatabaseConfig:
        """Load database configuration from environment variables."""
        return DatabaseConfig(
            host=os.getenv('H3XRECON_PROCESSOR_HOST', os.getenv('H3XRECON_DB_HOST', 'localhost')),
            port=int(os.getenv('H3XRECON_DB_PORT', '5432')),
            database=os.getenv('H3XRECON_DB_NAME', 'h3xrecon'),
            user=os.getenv('H3XRECON_DB_USER', 'postgres'),
            password=os.getenv('H3XRECON_DB_PASS', ''),
            min_size=int(os.getenv('H3XRECON_DB_MIN_CONN', '10')),
            max_size=int(os.getenv('H3XRECON_DB_MAX_CONN', '20'))
        )

    def _load_nats_config_env(self) -> NatsConfig:
        """Load NATS configuration from environment variables."""
        return NatsConfig(
            host=os.getenv('H3XRECON_PROCESSOR_HOST', os.getenv('H3XRECON_NATS_HOST', 'localhost')),
            port=int(os.getenv('H3XRECON_NATS_PORT', '4222')),
            user=os.getenv('H3XRECON_NATS_USER'),
            password=os.getenv('H3XRECON_NATS_PASSWORD')
        )

    def _load_redis_config_env(self) -> RedisConfig:
        """Load Redis configuration from environment variables."""
        return RedisConfig(
            host=os.getenv('H3XRECON_PROCESSOR_HOST', os.getenv('H3XRECON_REDIS_HOST', 'localhost')),
            port=int(os.getenv('H3XRECON_REDIS_PORT', '6379')),
            db=int(os.getenv('H3XRECON_REDIS_DB', '0')),
            password=os.getenv('H3XRECON_REDIS_PASSWORD')
        )

    def _load_log_config_env(self) -> LogConfig:
        """Load logging configuration from environment variables."""
        return LogConfig(
            level=os.getenv('H3XRECON_LOG_LEVEL', 'INFO'),
            format=os.getenv('H3XRECON_LOG_FORMAT', '<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>'),
            file_path=os.getenv('H3XRECON_LOG_FILE_PATH')
        )

    @classmethod
    def from_file(cls, config_path: str = 'config.json') -> 'Config':
        """Load configuration from a JSON file."""
        return cls(config_path)

    def setup_logging(self):
        """Configure logging based on current settings."""
        # Remove existing handlers
        logger.remove()
        
        # Add console handler
        logger.add(
            sink=lambda msg: print(msg),
            level=self.logging.level,
            format=self.logging.format
        )
        
        # Add file handler if configured
        if self.logging.file_path:
            logger.add(
                sink=self.logging.file_path,
                level=self.logging.level,
                format=self.logging.format,
                rotation="500 MB"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary format."""
        return {
            'database': self.database.to_dict(),
            'nats': {
                'host': self.nats.host,
                'port': self.nats.port,
                'url': self.nats.url
            },
            'redis': {
                'host': self.redis.host,
                'port': self.redis.port,
                'db': self.redis.db
            },
            'logging': {
                'level': self.logging.level,
                'file_path': self.logging.file_path
            },
            'worker_execution_threshold': self.worker_execution_threshold,
            'debug_mode': self.debug_mode
        }