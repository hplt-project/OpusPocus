import pytest

from opuspocus.utils import count_lines, file_line_index, open_file, read_shard


@pytest.mark.parametrize(
    "train_data_fixture",
    ["train_data_parallel_tiny", "train_data_parallel_tiny_decompressed"],
)
def test_count_lines(train_data_fixture, request):
    """Test line counting."""
    data = request.getfixturevalue(train_data_fixture)
    res = count_lines(data[0])
    assert res == 5  # noqa: PLR2004


@pytest.fixture(scope="function", params=["train_data_parallel_tiny"])
def sample_file(request):
    src_file, _ = request.getfixturevalue(request.param)
    return src_file


@pytest.fixture(scope="function")
def shard_index(sample_file):
    return file_line_index(sample_file)


def test_file_line_index_length(sample_file, shard_index):
    assert len(shard_index) == count_lines(sample_file)


@pytest.mark.parametrize("start,size", [(1, 2), (2, 3), (4, 5)])
def test_read_shard(sample_file, shard_index, start, size):
    lines = read_shard(sample_file, shard_index, start, size)
    with open_file(sample_file, "r",) as fh:
        for i, line in enumerate(fh):
            if i >= start and i < start + size:
                assert lines[i - start] == line
