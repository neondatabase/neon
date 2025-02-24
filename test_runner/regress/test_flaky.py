"""Test for detecting new flaky tests"""

import random


def test_flaky():
    assert random.random() < 0.95
