"""Test for detecting new flaky tests"""

import random


def test_flaky1():
    assert random.random() > 0.05


def no_test_flaky2():
    assert random.random() > 0.05
