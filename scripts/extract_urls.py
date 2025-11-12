
# Standard library
import json
import os
import time
from typing import Dict, Any

# Third-party
import streamlit as st
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    UnexpectedAlertPresentException,
)

# ----------------------------------------
# Page / app configuration
# ----------------------------------------
st.set_page_config(page_title="Weather Data Scraper - URL Extraction", layout="wide")
st.title("Weather Data Scraper - URL Extraction")

st.markdown(
    """
This app scrapes continent, country and city URLs using **Selenium 4** and **webdriver-manager**.
- ChromeDriver is auto-downloaded via `webdriver-manager` (or you can point to a cached binary).
"""
)

# ----------------------------------------
# Sidebar controls
# ----------------------------------------
with st.sidebar:
    st.header("Settings")
    MAX_COUNTRIES = st.number_input(
        "Max countries per continent (0 = all)", min_value=0, value=0, step=1
    )
    WAIT_TIMEOUT = st.number_input(
        "Selenium wait timeout (seconds)", min_value=1, value=10, step=1
    )
    DELAY = st.number_input(
        "Delay between requests (seconds)", min_value=0.0, value=0.3, step=0.1
    )
    RUN_BUTTON = st.button("Start scraping (Selenium)")
    st.markdown("---")
    st.write("Notes:")
    st.write("- Make sure Chrome is installed.")
    st.write("- On macOS, you may need to grant permission to run the downloaded chromedriver binary.")

# ----------------------------------------
# Constants
# ----------------------------------------
CONTINENTS: Dict[str, str] = {
    "North America": "https://en.climate-data.org/continent/north-america/",
    "South America": "https://en.climate-data.org/continent/south-america/",
    "Africa": "https://en.climate-data.org/continent/africa/",
    "Europe": "https://en.climate-data.org/continent/europe/",
    "Asia": "https://en.climate-data.org/continent/asia/",
    "Oceania": "https://en.climate-data.org/continent/oceania/",
}

# Path to a known working chromedriver binary (edit for your system if needed)
CHROMEDRIVER_CACHED_PATH = (
    "/Users/rahulmdinesh/.wdm/drivers/chromedriver/mac64/142.0.7444.61/"
    "chromedriver-mac-arm64/chromedriver"
)


# ----------------------------------------
# Helpers
# ----------------------------------------
def create_chrome_driver() -> webdriver.Chrome:
    """Create a visible (headed) Chrome WebDriver using a cached chromedriver path."""
    opts = Options()
    opts.page_load_strategy = "eager"  # return after DOMContentLoaded
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Use cached chromedriver path (fast and deterministic)
    chromedriver_path = CHROMEDRIVER_CACHED_PATH
    if not os.path.exists(chromedriver_path):
        # fallback to webdriver-manager if cached path missing
        chromedriver_path = ChromeDriverManager().install()

    try:
        os.chmod(chromedriver_path, 0o755)
    except Exception:
        # permission fix non-fatal; continue
        pass

    service = ChromeService(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=opts)

    # Try to ensure the window is visible and in front
    try:
        driver.maximize_window()
    except Exception:
        try:
            driver.set_window_size(1400, 900)
            driver.set_window_position(0, 0)
        except Exception:
            pass

    return driver


def extract_continents(driver: webdriver.Chrome, wait: WebDriverWait, max_countries: int = 0) -> Dict[str, Any]:
    """Return a nested structure: continent -> countries -> {url, cities{}}."""
    result = {name: {"url": url, "countries": {}} for name, url in CONTINENTS.items()}

    for cont_name, cont_info in result.items():
        url = cont_info["url"]
        try:
            driver.get(url)
            # wait for anchors under the list (returns list of <a> elements quickly)
            anchors = wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "ul.f16 li a[data-modified-href], ul.f16 li a"))
            )
            count = 0
            for a in anchors:
                href = a.get_attribute("href")
                text = a.text.strip()
                if text and href:
                    cont_info["countries"][text] = {"url": href, "cities": {}}
                    count += 1
                    if max_countries and count >= max_countries:
                        break
        except TimeoutException:
            st.warning(f"Timeout while loading continent page: {cont_name}")
        except Exception as exc:
            st.warning(f"Error loading continent {cont_name}: {exc}")

    return result


def extract_cities(driver: webdriver.Chrome, wait: WebDriverWait, result: Dict[str, Any], delay_s: float = 0.3) -> Dict[str, Any]:
    """For each country in result, populate the 'cities' dict with found city names and URLs."""
    total_countries = sum(len(c["countries"]) for c in result.values())
    processed = 0
    progress_bar = st.progress(0.0)
    status = st.empty()

    for cont_name, cont_info in result.items():
        for country_name, country_info in cont_info["countries"].items():
            status.text(f"Processing: {country_name} ({cont_name})")
            try:
                driver.get(country_info["url"])
                # wait for any table, then collect links from the 4th cell
                try:
                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                except TimeoutException:
                    # no table present; skip gracefully
                    continue

                # find all anchors in the 4th cell of table rows
                anchors = driver.find_elements(By.CSS_SELECTOR, "table tr td:nth-child(4) a")
                for a in anchors:
                    text = a.text.strip()
                    href = a.get_attribute("href")
                    if text and href:
                        country_info["cities"][text] = {"url": href}
            except UnexpectedAlertPresentException:
                # accept alert then continue
                try:
                    driver.switch_to.alert.accept()
                    time.sleep(0.5)
                except Exception:
                    pass
            except Exception:
                # ignore country-level errors and keep going
                pass

            processed += 1
            if total_countries:
                progress_bar.progress(min(processed / total_countries, 1.0))
            time.sleep(delay_s)

    status.text("Done.")
    return result


# ----------------------------------------
# Core scraping orchestration
# ----------------------------------------
def scrape_with_selenium(max_countries_per_continent: int = 0, wait_timeout_s: int = 10, delay_s: float = 0.3):
    driver = None
    try:
        driver = create_chrome_driver()
    except WebDriverException as e:
        st.error(f"Failed to create Chrome driver: {e}")
        return None

    wait = WebDriverWait(driver, wait_timeout_s)
    try:
        # Step 1 - continents -> countries
        result = extract_continents(driver, wait, max_countries=max_countries_per_continent)

        # Step 2 - countries -> cities
        result = extract_cities(driver, wait, result, delay_s=delay_s)

        return result
    finally:
        try:
            driver.quit()
        except Exception:
            pass


# ----------------------------------------
# UI Trigger & display
# ----------------------------------------
if RUN_BUTTON:
    with st.spinner("Scraper running - this will run in your session. Please be patient."):
        scraped = scrape_with_selenium(
            max_countries_per_continent=int(MAX_COUNTRIES),
            wait_timeout_s=int(WAIT_TIMEOUT),
            delay_s=float(DELAY),
        )

    if scraped is None:
        st.error("Scraping failed to start. See errors above.")
    else:
        st.success("Scraping finished.")

        # show nested JSON
        st.subheader("Nested JSON")
        st.json(scraped)

        # flatten to DataFrame
        rows = []
        for cont_name, cont_info in scraped.items():
            for country_name, country_info in cont_info["countries"].items():
                country_url = country_info.get("url")
                cities = country_info.get("cities", {})
                if cities:
                    for city_name, city_info in cities.items():
                        rows.append(
                            {
                                "continent": cont_name,
                                "country": country_name,
                                "country_url": country_url,
                                "city": city_name,
                                "city_url": city_info.get("url"),
                            }
                        )
                else:
                    rows.append(
                        {
                            "continent": cont_name,
                            "country": country_name,
                            "country_url": country_url,
                            "city": None,
                            "city_url": None,
                        }
                    )

        df = pd.DataFrame(rows)
        st.subheader("Flattened results")
        st.dataframe(df)

        # downloads
        json_str = json.dumps(scraped, indent=2, ensure_ascii=False)
        st.download_button("Download JSON", json_str, file_name="urls.json", mime="application/json")
        st.download_button("Download CSV", df.to_csv(index=False), file_name="urls.csv", mime="text/csv")
else:
    st.info("Configure settings and click 'Start scraping (Selenium)'")
