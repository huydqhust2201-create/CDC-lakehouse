import os

import pytest

# Keep tests off MinIO: everything runs on the local filesystem.
os.environ.setdefault("LAKEHOUSE_ROOT", "/tmp/lakehouse-tests")


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark_session import get_spark

    session = get_spark("lakehouse-tests", master="local[1]", s3=False)
    yield session
    session.stop()
