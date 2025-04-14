from bs4 import BeautifulSoup
from extensions.main import (
    get_category_from_soup,
    get_website_url_from_soup,
    check_if_detail_or_non_detail_page,
)


def test_get_category_from_soup():
    html = """
    <html>
        <body>
            <a href="/some-link">Extension</a>
            <a href="/category-link">Productivity</a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    category = get_category_from_soup(soup)
    assert category == "Productivity"


def test_get_website_url_from_soup_found():
    html = """
    <html>
        <body>
            <a href="https://example.com">Visit Website</a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    result = get_website_url_from_soup(soup)
    assert result == "https://example.com"


def test_get_website_url_from_soup_not_found():
    html = """
    <html>
        <body>
            <a href="https://example.com">Homepage</a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    result = get_website_url_from_soup(soup)
    assert result is None


def test_check_if_detail_page_true():
    url = "https://chromewebstore.google.com/detail/some-extension/abc123"
    assert check_if_detail_or_non_detail_page(url) is True


def test_check_if_detail_page_false():
    url = "https://chromewebstore.google.com/category/extensions/productivity"
    assert check_if_detail_or_non_detail_page(url) is False
