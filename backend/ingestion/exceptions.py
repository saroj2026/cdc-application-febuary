"""Custom exception classes for CDC pipeline."""

from __future__ import annotations


class CDCException(Exception):
    """Base exception for all CDC-related errors."""
    pass


class FullLoadError(CDCException):
    """Exception raised when full load fails."""
    
    def __init__(self, message: str, table_name: str = None, rows_transferred: int = 0, **kwargs):
        super().__init__(message)
        self.table_name = table_name
        self.rows_transferred = rows_transferred
        self.details = kwargs


class CDCError(CDCException):
    """Exception raised when CDC operations fail."""
    
    def __init__(self, message: str, connector_name: str = None, **kwargs):
        super().__init__(message)
        self.connector_name = connector_name
        self.details = kwargs


class ValidationError(CDCException):
    """Exception raised when data validation fails."""
    
    def __init__(self, message: str, validation_type: str = None, **kwargs):
        super().__init__(message)
        self.validation_type = validation_type
        self.details = kwargs


class ConnectionError(CDCException):
    """Exception raised when database connection fails."""
    
    def __init__(self, message: str, connection_id: str = None, **kwargs):
        super().__init__(message)
        self.connection_id = connection_id
        self.details = kwargs


class TransferError(CDCException):
    """Exception raised when data transfer fails."""
    
    def __init__(self, message: str, table_name: str = None, source: str = None, target: str = None, **kwargs):
        super().__init__(message)
        self.table_name = table_name
        self.source = source
        self.target = target
        self.details = kwargs
    """Base exception for all CDC-related errors."""
    pass


class FullLoadError(CDCException):
    """Exception raised when full load fails."""
    
    def __init__(self, message: str, table_name: str = None, rows_transferred: int = 0, **kwargs):
        super().__init__(message)
        self.table_name = table_name
        self.rows_transferred = rows_transferred
        self.details = kwargs


class CDCError(CDCException):
    """Exception raised when CDC operations fail."""
    
    def __init__(self, message: str, connector_name: str = None, **kwargs):
        super().__init__(message)
        self.connector_name = connector_name
        self.details = kwargs


class ValidationError(CDCException):
    """Exception raised when data validation fails."""
    
    def __init__(self, message: str, validation_type: str = None, **kwargs):
        super().__init__(message)
        self.validation_type = validation_type
        self.details = kwargs


class ConnectionError(CDCException):
    """Exception raised when database connection fails."""
    
    def __init__(self, message: str, connection_id: str = None, **kwargs):
        super().__init__(message)
        self.connection_id = connection_id
        self.details = kwargs


class TransferError(CDCException):
    """Exception raised when data transfer fails."""
    
    def __init__(self, message: str, table_name: str = None, source: str = None, target: str = None, **kwargs):
        super().__init__(message)
        self.table_name = table_name
        self.source = source
        self.target = target
        self.details = kwargs


