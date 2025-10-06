
import logging
import os

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        
        # Create a clear, informative format
        formatter = logging.Formatter(
            fmt='[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Default log level from ENV (default: INFO)
        level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(level)
        
    return logger
