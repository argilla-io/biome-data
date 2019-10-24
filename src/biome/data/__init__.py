import pkg_resources

try:
    __version__ = pkg_resources.get_distribution(
        __name__.replace(".", "-")).version
except pkg_resources.DistributionNotFound:
    # package is not installed
    pass

ENV_DASK_CLUSTER = "DASK_CLUSTER"
ENV_DASK_CACHE_SIZE = "DASK_CACHE_SIZE"
DEFAULT_DASK_CACHE_SIZE = 2e9

ENV_ES_HOSTS = "ES_HOSTS"
