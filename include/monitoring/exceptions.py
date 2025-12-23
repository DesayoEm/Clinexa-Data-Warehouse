class RequestExhaustionError(Exception):
    """
    Raised when a page fetch fails after exhausting all retry attempts.

    This exception is NOT raised by the requests library - it's raised by
    retry logic after max_attempts failed requests.

    This is a forced failure to prevent infinite retry loops. The underlying
    cause could be transient or persistent.

    Attributes:
        page_number: The page that failed to fetch
        max_attempts: Number of retry attempts that were exhausted
        url: The URL that failed to fetch
    """

    def __init__(self, page_number: int, max_attempts: int, url: str):
        message = f"Failed to fetch page {page_number} after {max_attempts} attempts. URL: {url} "
        super().__init__(message)
