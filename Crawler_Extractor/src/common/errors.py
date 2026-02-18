"""Domain-specific exception hierarchy for the crawler."""


class CrawlerError(Exception):
    """Base exception for known crawler failures."""


class ConfigError(CrawlerError):
    """Raised when required configuration is missing or invalid."""


class RepositoryError(CrawlerError):
    """Raised for Supabase repository failures."""


class DuplicateTopicError(RepositoryError):
    """Raised when attempting to insert a paper with an existing topic."""


class DuplicateTitleError(RepositoryError):
    """Raised when inserting a paper with a duplicate title."""


class StorageError(CrawlerError):
    """Raised when interacting with Supabase Storage."""


class ExtractionError(CrawlerError):
    """Raised when text extraction fails in a recoverable way."""


class PipelineError(CrawlerError):
    """Raised when orchestrator logic cannot continue."""
