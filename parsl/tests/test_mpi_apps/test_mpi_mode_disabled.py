from typing import Dict

import pytest

import parsl
from parsl import python_app
from parsl.executors.high_throughput.mpi_prefix_composer import (
    UnsupportedResourceSpecification,
)
from parsl.tests.configs.htex_local import fresh_config

EXECUTOR_LABEL = "MPI_TEST"


def local_config():
    config = fresh_config()
    config.executors[0].label = EXECUTOR_LABEL
    config.executors[0].max_workers_per_node = 1
    config.executors[0].enable_mpi_mode = False
    return config


@python_app
def hello_world(parsl_resource_specification: Dict = {}) -> str:
    return "hello world"


@pytest.mark.local
def test_only_resource_specs_set():
    """Confirm that resource_spec env vars result in an error when
    enable_mpi_mode == False"""
    resource_spec = {
        "num_nodes": 4,
        "ranks_per_node": 2,
    }

    with pytest.raises(UnsupportedResourceSpecification):
        hello_world(parsl_resource_specification=resource_spec).result()
