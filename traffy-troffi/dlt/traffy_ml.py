from datetime import datetime
from typing import Dict, Any

from dagster import Definitions, resource, asset, AssetExecutionContext, define_asset_job

from ..resources.gdrive import GoogleDriveResource
from ..resources.s3 import S3Resource


@resource(
    description="Google Drive resource for downloading traffy_ml model",
)
def traffy_ml_gdrive():
    return GoogleDriveResource(
        file_id="19QkF8i1my99gjbyHe7de_qZNwgrca6R5",
        mimetypes="text/csv"
    )


# Define asset for downloading a file
@asset(
    required_resource_keys={"traffy_ml_gdrive"},
    compute_kind="google-drive",
    group_name="traffy_ml",
    name="traffy_ml_model",
    description="Downloads traffy_ml model from Google Drive"
)
def traffy_ml_model(context) -> Dict[str, Any]:
    """Asset that downloads a large public file from Google Drive."""

    # Download the file
    context.log.info(f"Downloading file from Google Drive with ID: {context.resources.traffy_ml_gdrive.file_id}")
    result = context.resources.traffy_ml_gdrive.download_file()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    context.log.info(f"File size {result.getbuffer().nbytes / 1024:.2f} KB")
    return {
        "resource": result,
        "timestamp": timestamp,
    }


# Define an asset of the uploaded S3 model
@asset(
    required_resource_keys={"s3"},
    compute_kind="s3",
    group_name="traffy_ml",
    name="traffy_ml_model_s3",
    description="Uploads the downloaded model to S3"
)
def traffy_ml_model_s3(context: AssetExecutionContext, traffy_ml_model: Dict[str, Any]) -> Dict[str, str]:
    """Asset that uploads the downloaded model to S3."""

    # Upload the file to S3
    context.log.info("Uploading file to S3...")
    run_id = context.dagster_run.run_id
    s3: S3Resource = context.resources.s3
    output = s3.upload_file(
        file=traffy_ml_model['resource'],
        filename=f"traffy/fondue/traffy_ml_{traffy_ml_model['timestamp']}.csv",
        content_type="text/csv"
    )
    context.log.info(f"File uploaded to S3 successfully.")

    # Also upload to a "latest" version
    s3.get_client().copy_object(
        Bucket=output["Bucket"],
        CopySource=output,
        Key='traffy/fondue/traffy_ml_latest.csv',
        MetadataDirective="REPLACE",
        ContentType="text/csv"
    )
    context.log.info(f"File tagged as latest in S3 successfully.")
    # Return the DataFrame
    return output


# Define a job to download and upload the model
ingest_traffy_ml_model_job = define_asset_job(
    name="ingest_traffy_ml_model_job",
    description="Job to download and upload the traffy_ml model",
    selection=[traffy_ml_model, traffy_ml_model_s3],
)

# Create definitions object for deployment
traffy_model_defs = Definitions(
    assets=[traffy_ml_model, traffy_ml_model_s3],
    jobs=[ingest_traffy_ml_model_job],
    resources={"traffy_ml_gdrive": traffy_ml_gdrive},
)
