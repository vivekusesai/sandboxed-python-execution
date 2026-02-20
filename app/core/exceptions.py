"""Custom exception classes."""

from fastapi import HTTPException, status


class CredentialsException(HTTPException):
    """Exception for invalid credentials."""

    def __init__(self, detail: str = "Could not validate credentials"):
        super().__init__(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=detail,
            headers={"WWW-Authenticate": "Bearer"},
        )


class PermissionDenied(HTTPException):
    """Exception for permission denied."""

    def __init__(self, detail: str = "Permission denied"):
        super().__init__(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=detail,
        )


class NotFound(HTTPException):
    """Exception for resource not found."""

    def __init__(self, resource: str = "Resource"):
        super().__init__(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"{resource} not found",
        )


class BadRequest(HTTPException):
    """Exception for bad request."""

    def __init__(self, detail: str):
        super().__init__(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        )


class SandboxError(Exception):
    """Exception for sandbox execution errors."""

    def __init__(self, message: str, logs: str = ""):
        self.message = message
        self.logs = logs
        super().__init__(message)


class ValidationError(Exception):
    """Exception for data validation errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)
