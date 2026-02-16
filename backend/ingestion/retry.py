"""Retry utilities for handling transient failures."""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Type, Tuple

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Exception that indicates an operation should be retried."""
    pass


class NonRetryableError(Exception):
    """Exception that indicates an operation should not be retried."""
    pass


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """Retry decorator with exponential backoff.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
        exceptions: Tuple of exceptions to catch and retry
        on_retry: Optional callback function called on each retry (exception, attempt_number)
    
    Example:
        @retry(max_attempts=3, delay=1.0, backoff=2.0)
        def my_function():
            # This will be retried up to 3 times with exponential backoff
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    # Check if this is a non-retryable error
                    if isinstance(e, NonRetryableError):
                        logger.error(f"Non-retryable error in {func.__name__}: {e}")
                        raise
                    
                    if attempt < max_attempts:
                        logger.warning(
                            f"Attempt {attempt}/{max_attempts} failed for {func.__name__}: {e}. "
                            f"Retrying in {current_delay:.2f} seconds..."
                        )
                        
                        if on_retry:
                            try:
                                on_retry(e, attempt)
                            except Exception:
                                pass  # Don't fail on retry callback errors
                        
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"All {max_attempts} attempts failed for {func.__name__}: {e}"
                        )
                        raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
            raise Exception(f"Unexpected error in retry wrapper for {func.__name__}")
        
        return wrapper
    return decorator


def retry_on_connection_error(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """Retry decorator specifically for connection errors.
    
    Retries on common connection-related exceptions.
    """
    import psycopg2
    import requests
    
    # Make pyodbc optional (required for SQL Server, but not for PostgreSQL-only setups)
    try:
        import pyodbc
        pyodbc_available = True
    except ImportError:
        pyodbc_available = False
        logger.debug("pyodbc not available (SQL Server connections will not be retried)")
    
    connection_exceptions = [
        psycopg2.OperationalError,
        psycopg2.InterfaceError,
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.RequestException
    ]
    
    # Add pyodbc exceptions only if available
    if pyodbc_available:
        connection_exceptions.extend([
            pyodbc.OperationalError,
            pyodbc.InterfaceError
        ])
    
    connection_exceptions = tuple(connection_exceptions)
    
    return retry(
        max_attempts=max_attempts,
        delay=delay,
        backoff=backoff,
        exceptions=connection_exceptions
    )


def retry_on_timeout(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
):
    """Retry decorator specifically for timeout errors."""
    import requests
    
    timeout_exceptions = (
        requests.exceptions.Timeout,
        TimeoutError
    )
    
    return retry(
        max_attempts=max_attempts,
        delay=delay,
        backoff=backoff,
        exceptions=timeout_exceptions
    )


