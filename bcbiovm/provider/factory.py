"""Cloud provider factory."""
from bcbio.provider.aws import aws_provider

__all__ = ['get']
CLOUD_PROVIDER = {
    'aws': aws_provider.AWSProvider,
}


def get(cloud_provider):
    """Return the required cloud provider."""
    # TODO(alexandrucoman): Check if received name is valid
    return CLOUD_PROVIDER.get(cloud_provider)
