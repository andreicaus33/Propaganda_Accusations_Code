"""
Monitorul Oficial PII Scraper
Scrapes "Partea a II-a" (Part II) documents from Monitorul Oficial using Playwright.
"""
import asyncio
import calendar
import json
import logging
import random
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, Browser, TimeoutError as PlaywrightTimeoutError

from utils import generate_pdf_id, normalize_url, get_pdf_cache_path, ensure_cache_dirs


class MonitorulOfficialScraper:
    """
    Scraper for Monitorul Oficial Partea a II-a (PII) using Playwright.
    Properly navigates the e-monitor calendar by selecting year/month/day.
    Uses network monitoring to capture dynamically-loaded PDF URLs.
    """

    BASE_URL = "https://monitoruloficial.ro/"
    CALENDAR_URL = "https://monitoruloficial.ro/e-monitor/"

    def __init__(self, test_days: Optional[int] = None, debug_mode: bool = False):
        """
        Initialize scraper.

        Args:
            test_days: If set, only scrape first N days per month (for testing)
            debug_mode: If True, save HTML and link dumps to cache/debug/
        """
        self.logger = logging.getLogger(__name__)
        self.test_days = test_days
        self.debug_mode = debug_mode
        self._seen_issue_urls = set()  # Track seen issues to avoid duplicate warnings

        # Ensure cache directories exist
        ensure_cache_dirs()

        # Create debug directory if needed
        if debug_mode:
            self.debug_dir = Path('cache/debug')
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Debug mode enabled: saving to {self.debug_dir}")

    async def scrape_range(self, start_year: int, end_year: int) -> List[Dict]:
        """
        Scrape PDF URLs for a range of years.

        Args:
            start_year: First year to scrape (inclusive)
            end_year: Last year to scrape (inclusive)

        Returns:
            List of PDF metadata dictionaries
        """
        self.logger.info(f"Starting Monitorul Oficial scrape: {start_year}-{end_year}")
        if self.test_days:
            self.logger.info(f"TEST MODE: Only scraping first {self.test_days} days per month")

        all_pdfs = {}  # Use dict for deduplication by URL
        all_issues = {}  # Track issues by (year, month, day, issue_no)

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            # Create a browser context to share cookies between pages
            context = await browser.new_context(accept_downloads=True)

            try:
                for year in range(start_year, end_year + 1):
                    for month in range(1, 13):
                        self.logger.info(f"Scraping {year}-{month:02d}")

                        # Get number of days in this month
                        _, num_days = calendar.monthrange(year, month)

                        for day in range(1, num_days + 1):
                            scrape_date = date(year, month, day)

                            # Skip future dates
                            if scrape_date > date.today():
                                self.logger.debug(f"Skipping future date {scrape_date}")
                                continue

                            # Test mode: limit days per month
                            if self.test_days and day > self.test_days:
                                self.logger.info(f"Test mode: stopping at day {self.test_days}")
                                break

                            try:
                                issue_data = await self._scrape_date(context, scrape_date)

                                # Track issues found
                                for issue in issue_data:
                                    issue_key = (year, month, day, issue['issue_no'])
                                    if issue_key not in all_issues:
                                        all_issues[issue_key] = issue

                                    # Track PDFs
                                    for pdf_url in issue.get('pdf_urls', []):
                                        if pdf_url not in all_pdfs:
                                            all_pdfs[pdf_url] = {
                                                'pdf_id': generate_pdf_id(pdf_url),
                                                'source_pdf_url': pdf_url,
                                                'source': 'monitoruloficial_pii',
                                                'year': year,
                                                'month': month,
                                                'issue_no': issue['issue_no'],
                                                'issue_url': issue['issue_url']
                                            }

                                if issue_data:
                                    pdf_count = sum(len(issue.get('pdf_urls', [])) for issue in issue_data)
                                    self.logger.info(f"  {scrape_date}: {len(issue_data)} PII issues, {pdf_count} PDFs")

                            except Exception as e:
                                self.logger.warning(f"Failed to scrape {scrape_date}: {e}")

                            # Polite delay
                            await asyncio.sleep(random.uniform(1, 2))

            finally:
                await browser.close()

        self.logger.info(f"Collected {len(all_issues)} unique PII issues")
        self.logger.info(f"Collected {len(all_pdfs)} unique PDFs from Monitorul Oficial")
        return list(all_pdfs.values())

    async def _scrape_date(self, context, scrape_date: date) -> List[Dict]:
        """
        Scrape a single date for PII issues by navigating the e-monitor calendar.

        Args:
            context: Playwright browser context (shares cookies between pages)
            scrape_date: Date to scrape

        Returns:
            List of issue data dicts with 'issue_url', 'issue_no', 'year', 'pdf_urls'
        """
        page = await context.new_page()
        issues = []

        try:
            self.logger.debug(f"Navigating to e-monitor for {scrape_date}")

            # Navigate to calendar page
            await page.goto(self.CALENDAR_URL, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(2)  # Let page stabilize

            # Select year, month, and day on the calendar
            await self._select_date_on_calendar(page, scrape_date)

            # Wait for content to load
            await asyncio.sleep(2)
            await page.wait_for_load_state('networkidle', timeout=10000)

            # Extract PII issue links for this date
            issue_urls = await self._extract_pii_issues_for_date(page, scrape_date.year)

            if not issue_urls:
                self.logger.debug(f"No PII issues found for {scrape_date}")
                return []

            self.logger.debug(f"Found {len(issue_urls)} PII issues for {scrape_date}")

            # Download PDFs using Playwright (requires browser session with cookies)
            for issue_url, issue_no in issue_urls:
                try:
                    # Download the PDF using Playwright and save to cache
                    pdf_id = generate_pdf_id(issue_url)
                    local_path = get_pdf_cache_path(pdf_id)

                    # Check if already cached
                    if local_path.exists() and local_path.stat().st_size > 0:
                        self.logger.debug(f"  Issue {issue_no}: PDF already cached")
                        downloaded = True
                    else:
                        # Download using Playwright
                        downloaded = await self._download_pdf_with_playwright(
                            context, issue_url, local_path, issue_no
                        )

                    if downloaded:
                        issues.append({
                            'issue_url': issue_url,
                            'issue_no': issue_no,
                            'year': scrape_date.year,
                            'pdf_urls': [issue_url]  # Still return issue URL for metadata
                        })
                        self.logger.debug(f"  Issue {issue_no}: 1 PDF downloaded")
                    else:
                        self.logger.warning(f"  Issue {issue_no}: Failed to download PDF")

                except Exception as e:
                    self.logger.warning(f"  Issue {issue_no}: Error downloading PDF: {e}")

        finally:
            await page.close()

        return issues

    async def _select_date_on_calendar(self, page: Page, scrape_date: date):
        """
        Select year, month, and day on the e-monitor calendar.

        Args:
            page: Playwright page instance
            scrape_date: Date to select
        """
        year = scrape_date.year
        month = scrape_date.month
        day = scrape_date.day

        self.logger.debug(f"Selecting date: {year}-{month:02d}-{day:02d}")

        try:
            # Strategy: Look for datepicker elements and interact with them
            # The e-monitor page uses a calendar UI - we need to:
            # 1. Click on the date input/button to open calendar
            # 2. Navigate to correct year/month
            # 3. Click on the day

            # Try to find and click the datepicker trigger
            datepicker_selectors = [
                '#dayselect',
                'input[type="text"][id*="day"]',
                'input[type="text"][id*="date"]',
                '.datepicker',
                '[data-toggle="datepicker"]'
            ]

            datepicker_found = False
            for selector in datepicker_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        # Try to click to open calendar
                        await element.click()
                        await asyncio.sleep(0.5)
                        datepicker_found = True
                        self.logger.debug(f"Clicked datepicker: {selector}")
                        break
                except:
                    continue

            if not datepicker_found:
                self.logger.debug("No datepicker trigger found, trying direct date setting")

            # Set date via JavaScript (most reliable method for datepickers)
            date_str = scrape_date.strftime('%Y-%m-%d')

            success = await page.evaluate(f"""
                () => {{
                    let success = false;

                    // Try jQuery datepicker
                    if (typeof $ !== 'undefined') {{
                        const dp = $('#dayselect');
                        if (dp.length > 0 && dp.datepicker) {{
                            try {{
                                dp.datepicker('setDate', new Date({year}, {month - 1}, {day}));
                                dp.trigger('change');
                                success = true;
                                return 'jquery-datepicker';
                            }} catch (e) {{
                                console.log('jQuery datepicker failed:', e);
                            }}
                        }}
                    }}

                    // Try setting input values directly
                    const dateInputs = document.querySelectorAll('input[type="text"], input[type="date"]');
                    for (const input of dateInputs) {{
                        const id = (input.id || '').toLowerCase();
                        const name = (input.name || '').toLowerCase();
                        if (id.includes('day') || id.includes('date') || name.includes('day') || name.includes('date')) {{
                            input.value = '{date_str}';
                            input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            success = true;
                            return 'input-value';
                        }}
                    }}

                    // Try finding a form and setting data attributes
                    const forms = document.querySelectorAll('form');
                    for (const form of forms) {{
                        const dateField = form.querySelector('[name*="date"], [name*="day"], [id*="date"], [id*="day"]');
                        if (dateField) {{
                            dateField.value = '{date_str}';
                            dateField.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            success = true;
                            return 'form-field';
                        }}
                    }}

                    return success ? 'unknown' : false;
                }}
            """)

            if success:
                self.logger.debug(f"Date set successfully via: {success}")
            else:
                self.logger.debug("Could not set date via JavaScript, page may auto-show current date")

        except Exception as e:
            self.logger.debug(f"Date selection error: {e}")

    async def _download_pdf_with_playwright(
        self, context, issue_url: str, local_path: Path, issue_no: int
    ) -> bool:
        """
        Download a PDF from an issue URL using Playwright download handling.
        The issue URLs trigger direct PDF downloads when accessed with referer.

        Args:
            context: Playwright browser context (shares cookies)
            issue_url: URL that triggers PDF download
            local_path: Path to save the downloaded PDF
            issue_no: Issue number for logging

        Returns:
            True if download succeeded, False otherwise
        """
        page = await context.new_page()
        download_succeeded = False

        try:
            # Handle download event
            async with page.expect_download() as download_info:
                try:
                    # Navigate with referer - this triggers the download
                    # This will throw "Download is starting" error, but that's okay
                    await page.goto(issue_url, referer=self.CALENDAR_URL, timeout=30000)
                except Exception as e:
                    # "Download is starting" error is expected and okay
                    if "download" not in str(e).lower():
                        raise

            # Get the download object
            download = await download_info.value

            # Save to our cache location
            await download.save_as(local_path)

            # Verify it's a PDF
            if local_path.exists() and local_path.stat().st_size > 0:
                with open(local_path, 'rb') as f:
                    header = f.read(4)
                    if header == b'%PDF':
                        self.logger.debug(f"Downloaded PDF for issue {issue_no}: {local_path.stat().st_size} bytes")
                        download_succeeded = True
                    else:
                        self.logger.warning(f"Downloaded file for issue {issue_no} is not a PDF")
                        local_path.unlink()  # Delete non-PDF file
            else:
                self.logger.warning(f"Downloaded file for issue {issue_no} is empty")

            return download_succeeded

        except Exception as e:
            # Clean up on error
            if local_path.exists():
                local_path.unlink()
            self.logger.warning(f"Failed to download PDF for issue {issue_no}: {e}")
            return False

        finally:
            await page.close()

    async def _extract_pii_issues_for_date(self, page: Page, expected_year: int) -> List[Tuple[str, int]]:
        """
        Extract PII issue links from the page, filtering by expected year.
        Only returns links in "Partea a II-a" section with correct year.

        Args:
            page: Playwright page instance
            expected_year: Year we're scraping (to filter out wrong-year links)

        Returns:
            List of (issue_url, issue_no) tuples
        """
        issue_urls = []

        try:
            # Strategy: Find the "Partea a II-a" section, then extract links within it
            # Pattern: /Monitorul-Oficial--PII--{ISSUE_NO}--{YEAR}.html

            # Look for section header containing "Partea a II-a" or "PII"
            pii_section = None

            # Try to find the PII section by text content
            section_headers = await page.query_selector_all('h3, h4, h5, div.section-header, div[class*="title"]')

            for header in section_headers:
                text = await header.text_content()
                if text and ('partea a ii-a' in text.lower() or 'partea ii' in text.lower() or text.strip().lower() == 'pii'):
                    # Found the section header, get its parent container
                    pii_section = await header.evaluate_handle('el => el.closest("div, section") || el.parentElement')
                    self.logger.debug(f"Found PII section via header: {text.strip()}")
                    break

            # If no section found, search the whole page (fallback)
            if not pii_section:
                self.logger.debug("No PII section header found, searching entire page")
                pii_section = page

            # Extract all links within the PII section (or whole page if section not found)
            all_links = await page.query_selector_all('a.btn, a[href*="Monitorul-Oficial"]')

            # Pattern to match: Monitorul-Oficial--PII--{issue_no}--{year}.html
            # NOT Monitorul-Oficial--PIII-- (that's Partea a III-a)
            pii_pattern = re.compile(r'/Monitorul-Oficial--PII--(\d+)--(\d{4})\.html')

            for link in all_links:
                href = await link.get_attribute('href')
                if not href:
                    continue

                # Check if it matches PII pattern
                match = pii_pattern.search(href)
                if match:
                    issue_no = int(match.group(1))
                    link_year = int(match.group(2))

                    # CRITICAL: Only accept links matching the expected year
                    if link_year != expected_year:
                        self.logger.debug(f"Skipping wrong-year link: {href} (expected {expected_year}, got {link_year})")
                        continue

                    # Ensure it's not PIII (Partea a III-a)
                    if '--PIII--' in href or '--P3--' in href:
                        self.logger.debug(f"Skipping PIII link: {href}")
                        continue

                    absolute_url = normalize_url(href, self.BASE_URL)
                    issue_urls.append((absolute_url, issue_no))
                    self.logger.debug(f"Found PII issue {issue_no}/{link_year}: {absolute_url}")

        except Exception as e:
            self.logger.warning(f"Error extracting PII issues: {e}")

        # Deduplicate by URL
        seen = set()
        unique_issues = []
        for url, issue_no in issue_urls:
            if url not in seen:
                seen.add(url)
                unique_issues.append((url, issue_no))

        return unique_issues

    async def _extract_pdfs_from_issue_with_network(
        self, browser: Browser, issue_url: str, issue_no: int, year: int
    ) -> List[str]:
        """
        Extract PDF download links from a specific issue page.
        Uses network monitoring to capture dynamically-loaded PDF URLs.

        Args:
            browser: Playwright browser instance
            issue_url: URL to issue page
            issue_no: Issue number for debug output
            year: Year for debug output

        Returns:
            List of absolute PDF URLs
        """
        page = await browser.new_page()
        pdf_urls = set()
        network_pdfs = []  # PDFs found in network traffic

        # Set up network monitoring
        async def handle_response(response):
            """Capture network responses that might contain PDF URLs"""
            try:
                url = response.url
                url_lower = url.lower()

                # Check if response URL itself is a PDF
                if '.pdf' in url_lower or 'download' in url_lower:
                    self.logger.debug(f"Network: Found PDF URL in response: {url}")
                    network_pdfs.append(url)

                # Check if it's a JSON API response that might contain PDF URLs
                content_type = response.headers.get('content-type', '').lower()
                if 'json' in content_type:
                    try:
                        json_data = await response.json()
                        # Look for PDF URLs in JSON response
                        json_str = json.dumps(json_data).lower()
                        if '.pdf' in json_str or 'download' in json_str:
                            self.logger.debug(f"Network: JSON response contains PDF reference: {url}")
                            # Try to extract PDF URLs from JSON
                            self._extract_pdf_urls_from_json(json_data, network_pdfs)
                    except:
                        pass
            except Exception as e:
                self.logger.debug(f"Network monitoring error: {e}")

        page.on('response', handle_response)

        try:
            # Navigate to issue page with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await page.goto(issue_url, wait_until='networkidle', timeout=30000)
                    await asyncio.sleep(2)  # Wait for any dynamic content
                    break
                except PlaywrightTimeoutError:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

            # Strategy 1: Direct PDF links in DOM
            pdf_links = await page.query_selector_all('a[href$=".pdf"], a[href*=".pdf?"]')
            for link in pdf_links:
                href = await link.get_attribute('href')
                if href:
                    absolute_url = normalize_url(href, self.BASE_URL)
                    pdf_urls.add(absolute_url)

            # Strategy 2: Links with PDF-related text or classes
            all_links = await page.query_selector_all('a[href]')
            link_candidates = []

            for link in all_links:
                href = await link.get_attribute('href')
                if not href:
                    continue

                text = (await link.text_content() or '').strip()
                classes = (await link.get_attribute('class') or '')
                href_lower = href.lower()

                # Check for PDF indicators
                pdf_indicators = ['pdf', 'download', 'descarca', 'descarc', 'fisier', 'document']
                has_indicator = any(ind in text.lower() or ind in classes.lower() or ind in href_lower
                                   for ind in pdf_indicators)

                if has_indicator:
                    link_candidates.append({
                        'href': href,
                        'text': text[:50],
                        'classes': classes
                    })

                    if '.pdf' in href_lower:
                        absolute_url = normalize_url(href, self.BASE_URL)
                        pdf_urls.add(absolute_url)

            # Strategy 3: Check for embedded viewers or iframe sources
            iframes = await page.query_selector_all('iframe[src], embed[src], object[data]')
            for elem in iframes:
                src = await elem.get_attribute('src') or await elem.get_attribute('data')
                if src and '.pdf' in src.lower():
                    absolute_url = normalize_url(src, self.BASE_URL)
                    pdf_urls.add(absolute_url)

            # Strategy 4: Add PDFs found in network traffic
            for network_url in network_pdfs:
                absolute_url = normalize_url(network_url, self.BASE_URL)
                pdf_urls.add(absolute_url)

            # Debug mode: Save HTML and links if no PDFs found
            if not pdf_urls and self.debug_mode:
                await self._save_debug_info(page, issue_no, year, link_candidates, network_pdfs)

            # Log summary if no PDFs found
            if not pdf_urls:
                self.logger.debug(f"Issue {issue_no}/{year}: No PDFs found")
                self.logger.debug(f"  Link candidates: {len(link_candidates)}")
                self.logger.debug(f"  Network PDFs: {len(network_pdfs)}")
                if link_candidates:
                    self.logger.debug(f"  First candidate: {link_candidates[0]}")

        except Exception as e:
            self.logger.warning(f"Failed to extract PDFs from {issue_url}: {e}")

        finally:
            await page.close()

        return list(pdf_urls)

    def _extract_pdf_urls_from_json(self, json_data, network_pdfs: List[str]):
        """
        Recursively extract PDF URLs from JSON data.

        Args:
            json_data: JSON object (dict, list, or primitive)
            network_pdfs: List to append found PDF URLs to
        """
        try:
            if isinstance(json_data, dict):
                for key, value in json_data.items():
                    if isinstance(value, str) and ('.pdf' in value.lower() or 'download' in value.lower()):
                        # Check if it looks like a URL
                        if value.startswith('http') or value.startswith('/'):
                            network_pdfs.append(value)
                    else:
                        self._extract_pdf_urls_from_json(value, network_pdfs)
            elif isinstance(json_data, list):
                for item in json_data:
                    self._extract_pdf_urls_from_json(item, network_pdfs)
        except:
            pass

    async def _save_debug_info(
        self, page: Page, issue_no: int, year: int,
        link_candidates: List[Dict], network_pdfs: List[str]
    ):
        """
        Save debug information for an issue page.

        Args:
            page: Playwright page instance
            issue_no: Issue number
            year: Year
            link_candidates: List of link candidate dicts
            network_pdfs: List of PDF URLs found in network traffic
        """
        try:
            filename_base = f"issue_{issue_no}_{year}"

            # Save HTML
            html_path = self.debug_dir / f"{filename_base}.html"
            html_content = await page.content()
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"Saved debug HTML to {html_path}")

            # Save links
            links_path = self.debug_dir / f"{filename_base}_links.json"
            debug_data = {
                'issue_no': issue_no,
                'year': year,
                'url': page.url,
                'link_candidates': link_candidates,
                'network_pdfs': network_pdfs,
                'all_hrefs': []
            }

            # Get all hrefs on page
            all_hrefs = await page.evaluate("""
                () => Array.from(document.querySelectorAll('a[href]'))
                    .map(a => ({href: a.href, text: a.textContent.trim().substring(0, 50)}))
            """)
            debug_data['all_hrefs'] = all_hrefs[:20]  # First 20 only

            with open(links_path, 'w', encoding='utf-8') as f:
                json.dump(debug_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved debug links to {links_path}")

        except Exception as e:
            self.logger.warning(f"Failed to save debug info: {e}")


def scrape_monitorul_pdfs(start_year: int, end_year: int, test_days: Optional[int] = None, debug_mode: bool = False) -> List[Dict]:
    """
    Convenience function to scrape Monitorul Oficial PDFs.
    Wrapper to run async scraper synchronously.

    Args:
        start_year: First year to scrape
        end_year: Last year to scrape
        test_days: If set, only scrape first N days per month (for testing)
        debug_mode: If True, save HTML and link dumps to cache/debug/

    Returns:
        List of PDF metadata dictionaries
    """
    scraper = MonitorulOfficialScraper(test_days=test_days, debug_mode=debug_mode)
    return asyncio.run(scraper.scrape_range(start_year, end_year))
