import pytest

from opuspocus.utils import count_lines


@pytest.mark.parametrize(
    "train_data_fixture",
    ["train_data_parallel_tiny", "train_data_parallel_tiny_decompressed"],
)
def test_count_lines(train_data_fixture, request):
    """Test line counting."""
    data = request.getfixturevalue(train_data_fixture)
    res = count_lines(data[0])
    assert res == 5
