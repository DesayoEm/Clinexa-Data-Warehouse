import hashlib


def generate_key(*args) -> str:
    """Generates a deterministic surrogate key from input values."""
    normalized = (
        arg.lower() if isinstance(arg, str) else str(arg)
        for arg in args
        if arg is not None
    )
    combined = "|".join(normalized)
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]
