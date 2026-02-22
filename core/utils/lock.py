"""
Cross-platform file lock for single-instance enforcement.
Works on Windows, Mac, Linux.
"""

import os
import sys
import time
import errno
import logging

logger = logging.getLogger(__name__)

class CrossPlatformLock:
    """
    File-based lock with stale detection.
    
    Usage:
        lock = CrossPlatformLock('data/run.lock')
        if lock.acquire(timeout=30):
            try:
                # do work
            finally:
                lock.release()
        else:
            logger.error("Could not acquire lock")
    """
    
    def __init__(self, lock_path='data/run.lock', stale_minutes=10):
        self.lock_path = lock_path
        self.stale_minutes = stale_minutes
        self.fp = None
        self.pid = os.getpid()
    
    def acquire(self, timeout=30):
        """Acquire lock with timeout in seconds."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                if sys.platform == 'win32':
                    # Windows: use open with exclusive flags
                    self.fp = open(self.lock_path, 'w')
                    # Windows doesn't have fcntl, but we can use msvcrt
                    import msvcrt
                    msvcrt.locking(self.fp.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    # Unix: use fcntl
                    import fcntl
                    self.fp = open(self.lock_path, 'w')
                    fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                
                # Write PID to lock file for stale detection
                self.fp.write(str(self.pid))
                self.fp.flush()
                logger.debug(f"Lock acquired by PID {self.pid}")
                return True
                
            except (IOError, OSError, ImportError) as e:
                # Lock is held by another process
                if hasattr(e, 'errno') and e.errno in (errno.EAGAIN, errno.EACCES):
                    if self._is_stale():
                        logger.warning("Stale lock detected, removing")
                        self._remove_stale_lock()
                        continue
                
                logger.debug(f"Lock acquisition failed: {e}")
                time.sleep(1)
                
        logger.error(f"Timeout after {timeout}s waiting for lock")
        return False
    
    def _is_stale(self):
        """Check if lock file is older than stale_minutes."""
        try:
            if not os.path.exists(self.lock_path):
                return False
            
            # Check file age
            mtime = os.path.getmtime(self.lock_path)
            age_minutes = (time.time() - mtime) / 60
            
            if age_minutes > self.stale_minutes:
                # Try to read PID from stale lock
                try:
                    with open(self.lock_path, 'r') as f:
                        pid = f.read().strip()
                    logger.warning(f"Stale lock from PID {pid}, age {age_minutes:.1f}m")
                except:
                    pass
                return True
            
            return False
        except Exception as e:
            logger.error(f"Error checking stale lock: {e}")
            return False
    
    def _remove_stale_lock(self):
        """Remove stale lock file."""
        try:
            os.remove(self.lock_path)
            logger.info("Removed stale lock file")
        except Exception as e:
            logger.error(f"Failed to remove stale lock: {e}")
    
    def release(self):
        """Release the lock."""
        if self.fp:
            try:
                if sys.platform == 'win32':
                    import msvcrt
                    msvcrt.locking(self.fp.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
                
                self.fp.close()
                self.fp = None
                
                # Remove lock file
                if os.path.exists(self.lock_path):
                    os.remove(self.lock_path)
                    
                logger.debug("Lock released")
            except Exception as e:
                logger.error(f"Error releasing lock: {e}")
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()