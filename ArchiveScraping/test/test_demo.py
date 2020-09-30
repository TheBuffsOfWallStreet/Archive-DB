import pytest

@pytest.mark.xfail(reason="always xfail")
def test_xfail():
    assert(False)

def test_xpass():
    assert(True)
