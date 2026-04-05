"""Configure application-wide logging with a consistent structured format."""

import logging


# Set a single logging configuration so module logs are easy to correlate during streaming file processing.
def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
