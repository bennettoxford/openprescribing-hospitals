import pytest
from unittest.mock import patch
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def session_test_harness():
    with prefect_test_harness():
        yield


def pytest_configure(config):
    """This runs before tests are collected"""
    patcher = patch("prefect.blocks.system.Secret.load")
    mock_secret = patcher.start()
    mock_secret.return_value.get.return_value = "mock-trud-api-key"
