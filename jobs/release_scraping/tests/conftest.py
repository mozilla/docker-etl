import pytest

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

LOCAL_CHROME_BINARY = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


@pytest.fixture(scope="session")
def local_driver():
    """Selenium driver using the locally installed Chrome.

    Uses Selenium Manager for automatic ChromeDriver resolution so the driver
    version always matches the installed Chrome binary.
    """
    options = Options()
    options.binary_location = LOCAL_CHROME_BINARY
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    yield driver
    driver.quit()


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests against live APIs",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--integration"):
        skip = pytest.mark.skip(reason="Pass --integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
