import logging
import sys
import os

def setup_logger(name, log_file, console=True):
    """Set up a logger that writes to a specified file.

    Args:
        name (str): The name of the logger.
        log_file (str): The file to write logs to.
        console (bool): Whether to also log to the console.
    Returns:
        logging.Logger: Configured logger instance.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        file_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                "[%(levelname)s] %(name)s: %(message)s"
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

    return logger