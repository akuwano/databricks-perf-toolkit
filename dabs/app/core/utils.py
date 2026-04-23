"""
Utility functions for formatting and display.
"""


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if size_bytes == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    unit_index = 0
    size = float(size_bytes)
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    return f"{size:.2f} {units[unit_index]}"


def format_rows_human(rows: int) -> str:
    """Format row count to human-readable string (e.g., 1.95M, 13.20B)."""
    if rows < 1_000:
        return str(rows)
    elif rows < 1_000_000:
        return f"{rows / 1_000:.1f}K"
    elif rows < 1_000_000_000:
        return f"{rows / 1_000_000:.2f}M"
    elif rows < 1_000_000_000_000:
        return f"{rows / 1_000_000_000:.2f}B"
    else:
        return f"{rows / 1_000_000_000_000:.2f}T"


def format_time_ms(time_ms: int) -> str:
    """Format milliseconds to human-readable string."""
    if time_ms < 1000:
        return f"{time_ms} ms"
    elif time_ms < 60000:
        return f"{time_ms / 1000:.2f} sec"
    elif time_ms < 3600000:
        minutes = time_ms // 60000
        seconds = (time_ms % 60000) / 1000
        return f"{minutes} min {seconds:.1f} sec"
    else:
        hours = time_ms // 3600000
        minutes = (time_ms % 3600000) // 60000
        return f"{hours} hr {minutes} min"
