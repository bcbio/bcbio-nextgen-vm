"""Cloud provider factory."""
import collections

from bcbiovm.common import constant
from bcbiovm.provider.aws import aws_provider
from bcbiovm.provider import ship
from bcbiovm.provider.aws import ship as aws_ship

_Ship = collections.namedtuple("Ship", ["pack", "reconstitute"])

CLOUD_PROVIDER = {
    'aws': aws_provider.AWSProvider,
}
SHIP = {
    "shared": (None, ship.ReconstituteShared),
    "s3": (aws_ship.S3Pack, aws_ship.ReconstituteS3),
}


def get(cloud_provider=constant.DEFAULT_PROVIDER):
    """Return the required cloud provider."""
    # TODO(alexandrucoman): Check if received name is valid
    return CLOUD_PROVIDER.get(cloud_provider)


def get_ship(provider):
    """Return the ship required for the received provider."""
    # TODO(alexandrucoman): Check the received information
    return _Ship(*SHIP.get(provider))
