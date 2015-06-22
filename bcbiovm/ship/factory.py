"""Pack and Reconstitute objects factory."""
import collections
from bcbiovm.ship import pack
from bcbiovm.ship import reconstitute

__all__ = ['get']
Ship = collections.namedtuple("Ship", ["pack", "reconstitute"])
SHIP = {
    "shared": (None, reconstitute.ReconstituteShared),
    "s3": (pack.S3Pack, reconstitute.ReconstituteS3),
}


def get(provider):
    """Return the ship required for the received provider."""
    # TODO(alexandrucoman): Check the received information
    return Ship(*SHIP.get(provider))
