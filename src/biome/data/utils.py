import atexit
import logging
import os
import tempfile
from typing import Dict, Any, Union
from typing import Optional

import dask
import dask.multiprocessing
from dask.cache import Cache
from dask.distributed import Client, LocalCluster
from dask.utils import parse_bytes

from biome.data import ENV_DASK_CACHE_SIZE, DEFAULT_DASK_CACHE_SIZE

__LOGGER = logging.getLogger(__name__)

__DASK_CLIENT = None


def get_nested_property_from_data(data: Dict, property_key: str) -> Optional[Any]:
    """Search an deep property key in a data dictionary.

    For example, having the data dictionary {"a": {"b": "the value"}}, the call

    >> self.get_nested_property_from_data( {"a": {"b": "the value"}}, "a.b")

    is equivalent to:

    >> data["a"]["b"]


    Parameters
    ----------
    data
        The data dictionary
    property_key
        The (deep) property key

    Returns
    -------

        The property value if found, None otherwise
    """
    if data is None or not isinstance(data, Dict):
        return None

    if property_key in data:
        return data[property_key]

    sep = "."
    splitted_key = property_key.split(sep)

    return get_nested_property_from_data(
        data.get(splitted_key[0]), sep.join(splitted_key[1:])
    )


def configure_dask_cluster(
    address: str = "local", n_workers: int = 1, worker_memory: Union[str, int] = "1GB"
) -> Optional[Client]:
    """Creates a dask client (with a LocalCluster if needed)

    Parameters
    ----------
    address
        The cluster address. If "local" try to connect to a local cluster listening the 8786 port.
        If no cluster listening, creates a new LocalCluster
    n_workers
        The number of cluster workers (only a new "local" cluster generation)
    worker_memory
        The memory reserved for local workers

    Returns
    -------
    A new dask Client

    """
    global __DASK_CLIENT  # pylint: disable=global-statement

    if __DASK_CLIENT:
        return __DASK_CLIENT

    def create_dask_client(
        dask_cluster: str, cache_size: Optional[int], workers: int, worker_mem: int
    ) -> Client:
        if cache_size:
            cache = Cache(cache_size)
            cache.register()

        if dask_cluster == "local":
            try:
                return Client("localhost:8786", timeout=5)
            except OSError:
                dask.config.set(
                    {
                        "distributed.worker.memory": dict(
                            target=0.95, spill=False, pause=False, terminate=False
                        )
                    }
                )

                dask_data = os.path.join(os.getcwd(), ".dask")
                os.makedirs(dask_data, exist_ok=True)

                worker_space = tempfile.mkdtemp(dir=dask_data)
                cluster = LocalCluster(
                    n_workers=workers,
                    threads_per_worker=1,
                    asynchronous=False,
                    scheduler_port=8786,  # TODO configurable
                    processes=False,
                    memory_limit=worker_mem,
                    silence_logs=logging.ERROR,
                    local_directory=worker_space,
                )
                return Client(cluster)
        else:
            return dask.distributed.Client(dask_cluster)

    # import pandas as pd
    # pd.options.mode.chained_assignment = None

    dask_cluster = address
    dask_cache_size = os.environ.get(ENV_DASK_CACHE_SIZE, DEFAULT_DASK_CACHE_SIZE)

    if isinstance(worker_memory, str):
        worker_memory = parse_bytes(worker_memory)

    __DASK_CLIENT = create_dask_client(
        dask_cluster, dask_cache_size, workers=n_workers, worker_mem=worker_memory
    )

    return __DASK_CLIENT


@atexit.register
def close_dask_client():
    global __DASK_CLIENT  # pylint: disable=global-statement

    try:
        __DASK_CLIENT.close(timeout=10)
        __DASK_CLIENT.cluster.close(timeout=10)
    except Exception as err:  # pylint: disable=broad-except
        __LOGGER.debug(err)
    finally:
        __DASK_CLIENT = None
