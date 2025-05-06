import logging
from typing import Dict, Any, Optional, Union, BinaryIO
from pathlib import Path

import requests
from dagster import (
    ConfigurableResource
)

logger = logging.getLogger(__name__)

class ApiResource(ConfigurableResource):
    """Resource for flexible API interactions supporting JSON resources and file downloads"""
    base_url: str
    timeout: int = 30
    headers: Optional[Dict[str, str]] = None

    def fetch_json(self, endpoint: str = "", params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch JSON resources from the API"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        logger.info(f"Fetching JSON resources from {url}")

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self.timeout,
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching API resources: {e}")
            raise

    def download_file(
            self,
            endpoint: str = "",
            params: Optional[Dict[str, Any]] = None,
            output_path: Union[str, Path] = None,
            chunk_size: int = 8192
    ) -> Path:
        """
        Download a file from the API

        Args:
            endpoint: API endpoint to call
            params: Query parameters
            output_path: Path where to save the file (if None, will use filename from Content-Disposition or URL)
            chunk_size: Size of chunks for streaming download

        Returns:
            Path to the downloaded file
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}" if endpoint else self.base_url
        logger.info(f"Downloading file from {url}")

        try:
            response = requests.get(
                url,
                params=params,
                timeout=self.timeout,
                stream=True,
                headers=self.headers
            )
            response.raise_for_status()

            # Determine filename from Content-Disposition header or URL
            if output_path is None:
                if "Content-Disposition" in response.headers:
                    content_disposition = response.headers["Content-Disposition"]
                    filename = content_disposition.split("filename=")[-1].strip('"')
                else:
                    filename = os.path.basename(url.split("?")[0])
                output_path = Path(filename)
            else:
                output_path = Path(output_path)

            # Ensure directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Stream content to file
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            logger.info(f"File successfully downloaded to {output_path}")
            return output_path

        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file: {e}")
            raise