import io
from typing import Dict

from dagster import Definitions, job, resource, asset, AssetExecutionContext

from ..resources.gdrive import GoogleDriveResource


@resource(
    description="Google Drive resource for downloading traffy_fondue dataset",
)
def traffy_fondue_gdrive():
    return GoogleDriveResource(
        file_id="19QkF8i1my99gjbyHe7de_qZNwgrca6R5",
        mimetypes="text/csv"
    )


# Define asset for downloading a file
@asset(
    required_resource_keys={"traffy_fondue_gdrive"},
    compute_kind="google-drive",
    group_name="traffy_fondue",
    name="traffy_fondue_dataset",
    description="Downloads traffy_fondue dataset from Google Drive"
)
def traffy_fondue_dataset(context) -> io.BytesIO:
    """Asset that downloads a large public file from Google Drive."""

    # Download the file
    context.log.info(f"Downloading file from Google Drive with ID: {context.resources.traffy_fondue_gdrive.file_id}")
    result = context.resources.traffy_fondue_gdrive.download_file()

    context.log.info(f"File size {result.getbuffer().nbytes / 1024:.2f} KB")
    return result


# Define a job that uses the asset
@job(resource_defs={"traffy_fondue_gdrive": traffy_fondue_gdrive},
     description="Job to download raw traffy_fondue dataset as csv", )
def download_traffy_fondue_dataset():
    traffy_fondue_dataset()


# Define an asset of the uploaded S3 dataset
@asset(
    required_resource_keys={"s3"},
    compute_kind="s3",
    group_name="traffy_fondue",
    name="traffy_fondue_dataset_s3",
    description="Uploads the downloaded dataset to S3"
)
def traffy_fondue_dataset_s3(context: AssetExecutionContext, traffy_fondue_dataset: io.BytesIO) -> Dict[str, str]:
    """Asset that uploads the downloaded dataset to S3."""

    # Upload the file to S3
    context.log.info("Uploading file to S3...")
    output = context.resources.s3.upload_file(
        file=traffy_fondue_dataset,
        filename="/traffy/fondue/traffy_fondue.csv",
        content_type="text/csv"
    )
    context.log.info(f"File uploaded to S3 successfully.")
    # Return the DataFrame
    return output


# Define a job to upload the dataset to S3
@job(
    description="Job to upload the downloaded dataset to S3",
)
def upload_traffy_fondue_dataset(traffy_fondue_dataset: io.BytesIO):
    traffy_fondue_dataset_s3(traffy_fondue_dataset)


# Create definitions object for deployment
defs = Definitions(
    assets=[traffy_fondue_dataset, traffy_fondue_dataset_s3],
    jobs=[download_traffy_fondue_dataset, upload_traffy_fondue_dataset],
    resources={"traffy_fondue_gdrive": traffy_fondue_gdrive},
)
