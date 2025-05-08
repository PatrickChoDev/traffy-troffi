import io
import logging

import requests
from dagster import ConfigurableResource

logger = logging.getLogger(__name__)


def _get_confirm_token(response) -> str | None:
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None


class GoogleDriveResource(ConfigurableResource):
    file_id: str
    mimetypes: str
    chunk_size: int = 32768

    def _download_response(self) -> requests.Response:
        base_url = "https://docs.google.com/uc?export=download"
        session = requests.Session()

        # Initial request to get the HTML page
        response = session.get(base_url, params={"id": self.file_id}, stream=True)

        # Check if we got HTML instead of file content
        if "text/html" in response.headers.get("Content-Type", ""):
            logger.info("Received HTML page, extracting download link")

            # Parse the HTML content
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the form and extract the download URL
            form = soup.find('form')
            if form:
                action_url = form.get('action', base_url)

                # Collect all form inputs to submit
                params = {}
                for input_tag in form.find_all('input'):
                    if input_tag.get('name'):
                        params[input_tag.get('name')] = input_tag.get('value', '')

                # Submit the form using GET instead of POST
                logger.info(f"Submitting form with params: {params}")
                response = session.get(action_url, params=params, stream=True)
            else:
                # If no form found, try the direct download with the "confirm" parameter
                logger.info("No form found, trying direct download with confirm parameter")
                response = session.get(
                    base_url,
                    params={"id": self.file_id, "confirm": "t"},
                    stream=True,
                )

        return response

    def download_file(self):
        """
        Download a file from Google Drive using a streaming response.
        Handles HTML pages by extracting and submitting the download form with GET method.
        """
        response = self._download_response()

        # Double-check that we now have the file and not HTML
        if "text/html" in response.headers.get("Content-Type", ""):
            # Try to extract direct download link as a fallback
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for download links in the HTML
            download_link = None
            for a in soup.find_all('a', href=True):
                if 'download' in a['href'] and self.file_id in a['href']:
                    download_link = a['href']
                    break

            if download_link:
                logger.info(f"Found direct download link: {download_link}")
                response = requests.get(download_link, stream=True)
            else:
                raise RuntimeError(
                    "Failed to download file — received HTML page instead of file data and couldn't extract download link.")

        buffer = io.BytesIO()
        for chunk in response.iter_content(self.chunk_size):
            if chunk:  # filter out keep-alive chunks
                buffer.write(chunk)

        buffer.seek(0)
        return buffer
