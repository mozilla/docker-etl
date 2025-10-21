import importlib


def test_import_main():
    """Confirms main.py exists and is valid"""
    mod = importlib.import_module("release_scraping.main")
    assert hasattr(mod, "main")
