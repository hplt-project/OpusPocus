import pytest

from opuspocus.utils import *


@pytest.mark.parametrize(
    "train_data",
    [
        data_train_minimal,
        data_train_minimal_decompressed
    ]
)
def test_count_lines(train_data):
    """Test line counting."""
    res = count_lines(train_data[0])
    assert res == 5
