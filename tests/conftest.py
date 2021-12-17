import pytest


@pytest.fixture(autouse=True)
def assert_empty_output(capfd):
    yield

    captured = capfd.readouterr()

    assert captured.out == ''
    assert captured.err == ''
