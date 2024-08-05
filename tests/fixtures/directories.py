import pytest

from pathlib import Path


@pytest.fixture(scope="session")
def config_dir(tmp_path_factory):
    """Stores config files during testing."""
    return Path(tmp_path_factory.mktemp("config"))


@pytest.fixture(scope="session")
def marian_cpu_dir():
    """Location of CPU-compiled Marian NMT."""
    marian_dir = Path("marian_cpu")
    if not marian_dir.exists():
        pytest.skip(
            reason=(
                "A compiled CPU version of Marian NMT in 'marian_cpu_dir' "
                "must be available."
            )
        )
    return marian_dir


@pytest.fixture(scope="session")
def marian_gpu_dir():
    """Location of the GPU-compiled Marian NMT."""
    marian_dir = Path("marian_gpu")
    if not marian_dir.exists():
        pytest.skip(
            reason=(
                "A compiled GPU version of Marian NMT in 'marian_gpu_dir' "
                "must be available."
            )
        )
    return marian_dir


@pytest.fixture(scope="session")
def hyperqueue_dir():
    """Location of the HyperQueue program binary."""
    hq_dir = Path("hyperqueue")
    if not hq_dir.exists():
        pytest.skip(
            reason=("Hyperqueue binary must be located at " "{}/bin/hq.".format(hq_dir))
        )
    return hq_dir


@pytest.fixture(scope="session")
def opuspocus_hq_server_dir(tmp_path_factory):
    """Temporary directory to store HQ server information during tests."""
    return Path(tmp_path_factory.mktemp("opuspocus_hq_server"))
