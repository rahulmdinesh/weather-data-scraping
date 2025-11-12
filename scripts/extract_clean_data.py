# app.py - Streamlit + Selenium table scraper (cleaned & organized)

# Standard library
import json
import os
import re
import time
import tempfile
import shutil
import traceback
from typing import Dict, Optional

# Third-party
import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# -------------------------
# App config
# -------------------------
st.set_page_config(page_title="Weather Data Scraper - Data Extraction", layout="wide")
st.title("Weather Data Scraper - Data Extraction")
st.markdown(
    """
This app reads a saved `urls.json`, visits each city's page, extracts the `#weather_table` and returns a cleaned CSV file you can download.

**Notes**
- Chrome opens visibly while scraping (headed).
- The first run may download a chromedriver via `webdriver-manager`.
"""
)

# -------------------------
# Sidebar: input + controls
# -------------------------
st.sidebar.header("Input JSON")
uploaded = st.sidebar.file_uploader(
    "Upload urls.json (or leave empty and paste path below)", type=["json"]
)
json_path_input = st.sidebar.text_input("Or paste local file path of urls.json", value="")

st.sidebar.markdown("---")
max_countries = st.sidebar.number_input("Max countries per continent (0 = all)", min_value=0, value=0, step=1)
wait_timeout = st.sidebar.number_input("Selenium wait timeout (seconds)", min_value=3, value=10, step=1)
delay = st.sidebar.number_input("Delay between requests (seconds)", min_value=0.0, value=0.2, step=0.1)
run = st.sidebar.button("Start scraping")

# -------------------------
# Helper: load JSON of URLs
# -------------------------
def load_continents_json(uploaded_file, path_input: str) -> Optional[Dict]:
    """Return parsed JSON dict or None on failure."""
    if uploaded_file:
        try:
            return json.load(uploaded_file)
        except Exception as exc:
            st.sidebar.error(f"Failed to parse uploaded JSON: {exc}")
            return None
    if path_input:
        if not os.path.exists(path_input):
            st.sidebar.warning("Provided path does not exist.")
            return None
        try:
            with open(path_input, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception as exc:
            st.sidebar.error(f"Failed to read JSON file: {exc}")
            return None
    st.sidebar.info("Upload the JSON or paste a valid local path.")
    return None


continents_urls = load_continents_json(uploaded, json_path_input)

# -------------------------
# Driver factory (headed)
# -------------------------
# If you have a stable local chromedriver, you may set CHROMEDRIVER_PATH to speed startup.
CHROMEDRIVER_PATH = os.path.expanduser(
    "/Users/rahulmdinesh/.wdm/drivers/chromedriver/mac64/142.0.7444.61/chromedriver-mac-arm64/chromedriver"
)


def create_chrome_driver() -> webdriver.Chrome:
    """Create a visible Chrome WebDriver (Selenium 4)."""
    opts = Options()
    opts.page_load_strategy = "eager"  # return after DOMContentLoaded (faster)
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--log-level=3")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # prefer explicit local path if available, otherwise webdriver-manager
    chromedriver_path = CHROMEDRIVER_PATH if os.path.exists(CHROMEDRIVER_PATH) else ChromeDriverManager().install()

    try:
        os.chmod(chromedriver_path, 0o755)
    except Exception:
        # non-fatal: continue even if chmod fails
        pass

    service = ChromeService(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=opts)

    # best-effort bring window into view
    try:
        driver.maximize_window()
    except Exception:
        try:
            driver.set_window_size(1400, 900)
            driver.set_window_position(0, 0)
        except Exception:
            pass

    return driver


# -------------------------
# Data cleaning helpers
# -------------------------
def clean_data(df: pd.DataFrame, continent: str, country: str, city: str) -> pd.DataFrame:
    """
    Transpose table, set header row, add metadata columns (continent/country/city/month),
    and ensure avg. Sun hours column exists (filled with 0 if missing).
    """
    df_clean = df.transpose().copy()
    df_clean.columns = df_clean.iloc[0]
    df_clean = df_clean.iloc[1:].reset_index(drop=True)

    df_clean["Continent"] = continent
    df_clean["Country"] = country
    df_clean["City"] = city
    df_clean["Month"] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    # ensure avg. Sun hours column exists and has numeric values
    if "avg. Sun hours (hours)" in df_clean.columns:
        df_clean["avg. Sun hours (hours)"] = df_clean["avg. Sun hours (hours)"].fillna(0)
    else:
        df_clean["avg. Sun hours (hours)"] = 0

    return df_clean

def clean_temperature(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    From a column like 'Avg. Temperature °C (°F)' extract numeric Celsius and Fahrenheit
    into new columns and drop the original column. Robust to missing column.
    """
    if column not in df.columns:
        return df

    # try to extract a short label; if not found, use the column name (safe)
    m = re.search(r'^(Avg|Min|Max)\.?\s*Temperature', column, flags=re.IGNORECASE)
    temperature_label = m.group(0) if m else column

    s = df[column].astype(str).str.replace("\n", " (", regex=False)
    s = s.apply(lambda v: v + ")" if not v.endswith(")") else v)

    # Extract Fahrenheit inside parentheses: e.g. "(71.4) °F"
    df[f"{temperature_label} (°F)"] = s.str.extract(r'\((-?[0-9\.]+)\)\s*°F', expand=False)
    # Extract Celsius at start: e.g. "21.9 °C"
    df[f"{temperature_label} (°C)"] = s.str.extract(r'^(-?[0-9\.]+)\s*°C', expand=False)

    return df.drop(columns=[column])


def clean_precipitation(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Split precipitation like '40 (1.6)' into mm and in columns; drop original."""
    if column not in df.columns:
        return df
    s = df[column].astype(str)
    df["Precipitation / Rainfall (mm)"] = df[column].str.extract(r'^(\d+)', expand=False) 
    df["Precipitation / Rainfall (in)"] = df[column].str.extract(r'\((\d+)\)', expand=False) 
    df = df.drop(columns=[column])
    return df


def clean_humidity(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Extract numeric percent from humidity like '82%'."""
    if column not in df.columns:
        return df
    df[column] = df[column].str.extract(r'(\d+)', expand=False) 
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns to a stable schema; missing columns are created (as None)."""
    desired = [
        "Continent",
        "Country",
        "City",
        "Month",
        "Avg. Temperature (°F)",
        "Avg. Temperature (°C)",
        "Min. Temperature (°F)",
        "Min. Temperature (°C)",
        "Max. Temperature (°F)",
        "Max. Temperature (°C)",
        "Precipitation / Rainfall (in)",
        "Precipitation / Rainfall (mm)",
        "Humidity(%)",
        "Rainy days (d)",
        "avg. Sun hours (hours)",
    ]
    for col in desired:
        if col not in df.columns:
            df[col] = None
    return df[desired]


# -------------------------
# Main scraping flow
# -------------------------
if run:
    if not continents_urls:
        st.error("No valid JSON provided. Upload or paste path in the sidebar.")
    else:
        completed = pd.DataFrame()
        total_cities = sum(
            len(country.get("cities", {}))
            for cont in continents_urls.values()
            for country in cont.get("countries", {}).values()
        )

        progress = st.progress(0.0)
        status = st.empty()

        driver: Optional[webdriver.Chrome] = None
        try:
            driver = create_chrome_driver()
            wait = WebDriverWait(driver, wait_timeout)

            processed = 0
            # iterate all cities
            for continent_name, continent_info in continents_urls.items():
                for country_name, country_info in continent_info.get("countries", {}).items():
                    for city_name, city_info in country_info.get("cities", {}).items():
                        processed += 1
                        status.text(
                            f"Processing {city_name} - {country_name}, {continent_name} ({processed}/{total_cities})"
                        )

                        url = city_info.get("url")
                        if not isinstance(url, str) or not url:
                            st.warning(f"Invalid URL for {city_name}. Skipping.")
                            continue

                        try:
                            driver.get(url)

                            # first try to find #weather_table, else fallback to any table
                            table_html = None
                            try:
                                wait.until(EC.presence_of_element_located((By.ID, "weather_table")))
                                table_html = driver.find_element(By.ID, "weather_table").get_attribute("outerHTML")
                            except TimeoutException:
                                try:
                                    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                                    table_html = driver.find_element(By.TAG_NAME, "table").get_attribute("outerHTML")
                                except TimeoutException:
                                    st.warning(f"No weather table found for {city_name}.")
                                    continue

                            # parse table HTML with BeautifulSoup
                            soup = BeautifulSoup(table_html, "html.parser")
                            table = soup.find("table")
                            if table is None:
                                st.warning(f"Could not parse table for {city_name}.")
                                continue

                            # collect text cells into list-of-lists
                            rows = []
                            thead = table.find("thead")
                            tbody = table.find("tbody")
                            all_rows = []
                            if thead:
                                all_rows.extend(thead.find_all("tr"))
                            if tbody:
                                all_rows.extend(tbody.find_all("tr"))
                            if not all_rows:
                                all_rows = table.find_all("tr")

                            for tr in all_rows:
                                cols = [td.get_text(strip=True) for td in tr.find_all("td")]
                                # keep only non-empty columns (preserve order)
                                rows.append([c for c in cols if c != ""])

                            if not rows:
                                st.warning(f"No usable data in table for {city_name}.")
                                continue

                            df = pd.DataFrame(rows)
                            df_clean = clean_data(df, continent_name, country_name, city_name)
                            completed = pd.concat([completed, df_clean], ignore_index=True)

                        except Exception as exc:
                            st.warning(f"Error scraping {city_name}: {exc}")
                            traceback.print_exc()

                        # polite delay and progress update
                        time.sleep(delay)
                        if total_cities > 0:
                            progress.progress(min(processed / total_cities, 1.0))

            status.text("Finished scraping - post-processing...")

            # post-process
            if not completed.empty:
                # clean temperature columns (if present)
                temp_columns = ["Avg. Temperature °C (°F)", "Min. Temperature °C (°F)", "Max. Temperature °C (°F)"]
                for col in temp_columns:
                    completed = clean_temperature(completed, col)

                completed = clean_precipitation(completed, "Precipitation / Rainfall mm (in)")
                completed = clean_humidity(completed, "Humidity(%)")
                completed = reorder_columns(completed)

                st.success("Scraping complete. Preview below.")
                st.dataframe(completed.head(200))

                # save CSV to temp file and allow download
                tmpdir = tempfile.mkdtemp(prefix="weather_scraping_out_")
                out_path = os.path.join(tmpdir, "cleaned_data.csv")
                completed.to_csv(out_path, index=False, encoding="utf-8", sep=";")
                with open(out_path, "rb") as fh:
                    st.download_button("Download CSV", fh, file_name="cleaned_data.csv", mime="text/csv")
                # clean up temp dir
                try:
                    shutil.rmtree(tmpdir)
                except Exception:
                    pass
            else:
                st.info("No data was collected.")
        finally:
            # ensure driver quits cleanly
            try:
                if driver:
                    driver.quit()
                    time.sleep(0.6)  # allow macOS to settle the Dock icon
            except Exception:
                pass
