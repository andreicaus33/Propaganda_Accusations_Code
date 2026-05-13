"""
Scraper for Romania Senate stenogram PDFs using HTTP requests only.
"""
import logging
import time
from typing import Dict, List, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils import generate_pdf_id, normalize_url, is_pdf_url


class SenateScraper:
    """
    Scraper for Romania Senate website to collect stenogram PDF links.
    Uses requests + BeautifulSoup without browser automation.
    """

    BASE_URL = "https://www.senat.ro/"
    CALENDAR_URL = "https://www.senat.ro/stenogramecalendar.aspx?Plen=1"

    def __init__(self, session: requests.Session = None):
        """
        Initialize scraper.

        Args:
            session: Optional requests session (creates new if None)
        """
        self.logger = logging.getLogger(__name__)
        self.session = session or requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def scrape_year_month(self, year: int, month: int) -> List[str]:
        """
        Scrape stenogram page for a specific year and month.

        Args:
            year: Year to scrape
            month: Month to scrape (1-12)

        Returns:
            List of PDF URLs found
        """
        self.logger.info(f"Scraping {year}-{month:02d}")

        pdf_urls = set()

        # Scrape from the stenogram calendar page
        try:
            pdf_urls.update(self._scrape_calendar_page(year, month))
        except Exception as e:
            self.logger.debug(f"Failed to scrape calendar page: {e}")

        return list(pdf_urls)

    def _scrape_calendar_page(self, year: int, month: int) -> Set[str]:
        """
        Scrape the stenogram calendar page for a specific year.

        The calendar shows all months for the year, we filter for the requested month.

        Args:
            year: Year to filter
            month: Month to filter

        Returns:
            Set of PDF URLs
        """
        try:
            # The calendar URL pattern for specific years
            url = f"https://www.senat.ro/stenogramecalendar.aspx?Plen=1&An={year}"
            self.logger.debug(f"Fetching calendar: {url}")

            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                self.logger.warning(f"Failed to fetch calendar for {year}: status {response.status_code}")
                return set()

            soup = BeautifulSoup(response.text, 'html.parser')
            pdf_urls = set()

            # Find all links to stenogram PDFs
            # Patterns across years:
            # 2020 and earlier: PAGINI/Stenograme/StenogrameYYYY/YY.MM.DD.pdf
            # 2024 and later: PAGINI/Stenograme/Stenograme_YYYY/Plen/YY.MM.DD.pdf
            for link in soup.find_all('a', href=True):
                href = link['href']

                # Check if this is a stenogram PDF link
                # Must contain both 'Stenograme' folder and current year
                if 'Stenograme' in href and str(year) in href and href.endswith('.pdf'):
                    # Extract date from PDF filename (format: YY.MM.DD.pdf or YY.MM.DD ex.pdf)
                    try:
                        filename = href.split('/')[-1]  # Get filename
                        # Remove .pdf and any extra text like ' ex', ' e', etc.
                        base_name = filename.replace('.pdf', '').strip()

                        # Try to extract date from the beginning of the filename
                        # Format: YY.MM.DD (possibly followed by space and session type)
                        parts = base_name.split()
                        date_str = parts[0]  # Get first part (the date)
                        date_parts = date_str.split('.')

                        if len(date_parts) >= 2:
                            # Parse month from YY.MM.DD
                            pdf_month = int(date_parts[1])

                            # Only include if it matches our target month
                            if pdf_month == month:
                                absolute_url = normalize_url(href, self.BASE_URL)
                                pdf_urls.add(absolute_url)
                                self.logger.debug(f"Found stenogram: {absolute_url}")
                    except (ValueError, IndexError) as e:
                        self.logger.debug(f"Could not parse date from {href}: {e}")
                        continue

            return pdf_urls

        except Exception as e:
            self.logger.warning(f"Failed to scrape calendar page for {year}-{month:02d}: {e}")
            return set()

    def _extract_pdf_urls_from_page(self, html: str, page_url: str) -> Set[str]:
        """
        Extract PDF URLs from an HTML page.

        Args:
            html: HTML content
            page_url: URL of the page (for resolving relative links)

        Returns:
            Set of absolute PDF URLs
        """
        soup = BeautifulSoup(html, 'html.parser')
        pdf_urls = set()

        # Find all anchor tags
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = normalize_url(href, page_url)

            # Direct PDF link
            if is_pdf_url(absolute_url):
                pdf_urls.add(absolute_url)
                self.logger.debug(f"Found direct PDF: {absolute_url}")

            # Potential detail page that might contain PDF links
            elif self._is_potential_detail_page(absolute_url):
                # Fetch the detail page and extract PDF links
                detail_pdfs = self._extract_pdfs_from_detail_page(absolute_url)
                pdf_urls.update(detail_pdfs)

        return pdf_urls

    def _is_potential_detail_page(self, url: str) -> bool:
        """
        Check if URL might be a detail page containing PDF links.

        Args:
            url: URL to check

        Returns:
            True if URL looks like a detail page
        """
        # Common patterns for detail pages on senat.ro
        detail_indicators = [
            'detalii', 'details', 'sedinta', 'stenograma',
            'programbp', 'programf', 'activitate'
        ]
        url_lower = url.lower()
        return any(indicator in url_lower for indicator in detail_indicators)

    def _extract_pdfs_from_detail_page(self, detail_url: str) -> Set[str]:
        """
        Fetch a detail page and extract PDF links from it.

        Args:
            detail_url: URL of detail page

        Returns:
            Set of PDF URLs found on detail page
        """
        try:
            self.logger.debug(f"Checking detail page: {detail_url}")
            response = self.session.get(detail_url, timeout=30)
            if response.status_code != 200:
                return set()

            soup = BeautifulSoup(response.text, 'html.parser')
            pdf_urls = set()

            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = normalize_url(href, detail_url)
                if is_pdf_url(absolute_url):
                    pdf_urls.add(absolute_url)
                    self.logger.debug(f"Found PDF in detail page: {absolute_url}")

            time.sleep(0.5)  # Be polite
            return pdf_urls

        except Exception as e:
            self.logger.warning(f"Failed to fetch detail page {detail_url}: {e}")
            return set()

    def scrape_range(self, start_year: int, end_year: int) -> List[Dict]:
        """
        Scrape PDF URLs for a range of years.

        Args:
            start_year: Start year (inclusive)
            end_year: End year (inclusive)

        Returns:
            List of PDF metadata dictionaries
        """
        all_pdfs = {}  # Use dict to deduplicate by URL

        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                try:
                    pdf_urls = self.scrape_year_month(year, month)

                    for url in pdf_urls:
                        if url not in all_pdfs:
                            all_pdfs[url] = {
                                'pdf_id': generate_pdf_id(url),
                                'source_pdf_url': url,
                                'year': year,
                                'month': month
                            }

                    self.logger.info(
                        f"Scraped {year}-{month:02d}: {len(pdf_urls)} PDFs "
                        f"(total unique: {len(all_pdfs)})"
                    )

                    time.sleep(1)  # Be respectful to the server

                except Exception as e:
                    self.logger.error(f"Error scraping {year}-{month:02d}: {e}")
                    continue

        pdf_list = list(all_pdfs.values())
        self.logger.info(f"Total unique PDFs collected: {len(pdf_list)}")
        return pdf_list


def scrape_pdfs(start_year: int, end_year: int) -> List[Dict]:
    """
    Convenience function to scrape PDFs for a year range.

    Args:
        start_year: Start year
        end_year: End year

    Returns:
        List of PDF metadata dictionaries
    """
    scraper = SenateScraper()
    return scraper.scrape_range(start_year, end_year)
