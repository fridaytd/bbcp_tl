import time
from datetime import datetime

from app import logger


def sleep_for(delay: float) -> None:
    logger.info(f"Sleep for {delay} seconds")
    time.sleep(delay)


def formated_datetime(
    now: datetime,
) -> str:
    formatted_date = now.strftime("%d/%m/%Y %H:%M:%S")
    return formatted_date


def split_list(lst: list, chunk_size: int) -> list[list]:
    """
    Split a list into smaller chunks of specified size

    Args:
        lst (list): Input list to split
        chunk_size (int): Size of each chunk

    Returns:
        list: List containing sublists of specified chunk size
    """
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]
