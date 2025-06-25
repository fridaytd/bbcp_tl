import logging

from .shared.config import Config

## Seting logger
# Configure logging once at the application level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s :: %(message)s",
    handlers=[logging.StreamHandler()],
)

# Get logger for this module
logger = logging.getLogger(__name__)


config = Config.from_env()


__all__ = ["config", "logger"]
