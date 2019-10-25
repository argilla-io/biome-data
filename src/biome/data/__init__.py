import pkg_resources

from .sources import DataSource

try:
    __version__ = pkg_resources.get_distribution(__name__.replace(".", "-")).version
except pkg_resources.DistributionNotFound:
    # package is not installed
    pass

# TODO : remove from here and/or remove if not needed
ENV_DASK_CACHE_SIZE = "DASK_CACHE_SIZE"
DEFAULT_DASK_CACHE_SIZE = 2e9
