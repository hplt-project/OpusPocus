import pytest

from opuspocus.utils import count_lines


@pytest.mark.parametrize(
    "train_data_fixture", ["data_train_minimal", "data_train_minimal_decompressed"]
)
def test_count_lines(train_data_fixture, request):
    """Test line counting."""
    data = request.getfixturevalue(train_data_fixture)
    res = count_lines(data[0])
    assert res == 5
