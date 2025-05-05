import logging
from typing import Dict, Any

import requests
from dagster import (
    ConfigurableResource
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Resource classes
class ApiResource(ConfigurableResource):
    """Resource for API interactions"""
    base_url: str
    timeout: int = 30

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch data from the API"""
        logger.info(f"Fetching data from {self.base_url}")
        try:
            response = requests.get(self.base_url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching API data: {e}")
            raise