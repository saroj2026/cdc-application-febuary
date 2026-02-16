"""Database logging handler for storing application logs in the database."""

import logging
import traceback
from datetime import datetime, timezone
from typing import Optional
import threading
from queue import Queue

try:
    from ingestion.database.session import SessionLocal
    from ingestion.database.models_db import ApplicationLogModel
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False


class DatabaseLogHandler(logging.Handler):
    """Custom logging handler that stores logs in the database."""
    
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)
        self.log_queue = Queue(maxsize=1000)  # Buffer up to 1000 logs
        self.worker_thread = None
        self._start_worker()
    
    def _start_worker(self):
        """Start background worker thread to process log queue."""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
            self.worker_thread.start()
    
    def emit(self, record: logging.LogRecord):
        """Emit a log record to the database queue."""
        try:
            # Don't block if queue is full
            if not self.log_queue.full():
                self.log_queue.put_nowait(record)
        except Exception:
            # Silently fail to avoid logging errors causing infinite loops
            pass
    
    def _process_queue(self):
        """Background worker to process log queue and store in database."""
        while True:
            try:
                # Wait for log record with timeout
                record = self.log_queue.get(timeout=1.0)
                
                if not DB_AVAILABLE:
                    continue
                
                try:
                    db = SessionLocal()
                    try:
                        # Extract log information
                        log_entry = ApplicationLogModel(
                            level=record.levelname,
                            logger=record.name,
                            message=self.format(record),
                            module=record.module if hasattr(record, 'module') else None,
                            function=record.funcName if hasattr(record, 'funcName') else None,
                            line=record.lineno if hasattr(record, 'lineno') else None,
                            timestamp=datetime.fromtimestamp(record.created, tz=timezone.utc),
                            extra=self._extract_extra(record)
                        )
                        
                        db.add(log_entry)
                        db.commit()
                    except Exception as e:
                        db.rollback()
                        # Don't log database errors to avoid infinite loops
                        pass
                    finally:
                        db.close()
                except Exception:
                    # Silently fail if database is not available
                    pass
                
                self.log_queue.task_done()
            except Exception:
                # Continue processing even if one log fails
                continue
    
    def _extract_extra(self, record: logging.LogRecord) -> Optional[dict]:
        """Extract extra information from log record."""
        extra = {}
        
        # Add exception info if present
        if record.exc_info:
            extra['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': traceback.format_exception(*record.exc_info) if record.exc_info else None
            }
        
        # Add any custom attributes
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'created', 'filename', 'funcName', 
                          'levelname', 'levelno', 'lineno', 'module', 'msecs', 
                          'message', 'pathname', 'process', 'processName', 'relativeCreated',
                          'thread', 'threadName', 'exc_info', 'exc_text', 'stack_info']:
                try:
                    # Only include JSON-serializable values
                    import json
                    json.dumps(value)
                    extra[key] = value
                except (TypeError, ValueError):
                    extra[key] = str(value)
        
        return extra if extra else None

