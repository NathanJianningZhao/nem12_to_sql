class ValidationError(Exception):
    """Base class for validation-related errors."""


class FatalFileError(ValidationError):
    """Raised when the file structure is invalid and processing should stop."""


class RecoverableRecordError(ValidationError):
    """Raised when a single record is invalid and can be skipped."""