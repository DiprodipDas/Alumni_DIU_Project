import pandas as pd
import random
import time
import json
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
EMAIL = os.getenv("LINKEDIN_EMAIL")
PASSWORD = os.getenv("LINKEDIN_PASSWORD")

# ==============================
# CONFIG
# ==============================
INPUT_FILE = "onlytwentyen.csv"  # must have column "Linkedin_Link"
OUTPUT_FILE = "linkedin_profilesss.csv"
BATCH_SIZE = 5  # profiles per run
DELAY_BETWEEN_PROFILES = (30, 90)  # seconds (min, max)
# ==============================
# HELPERS
# ==============================
def normalize_link(url: str) -> str:
    if not isinstance(url, str) or "linkedin.com" not in url:
        return None
    parsed = urlparse(url)
    path = parsed.path
    return f"https://www.linkedin.com{path}".split("?")[0]

# ==============================
# LOGIN FUNCTION
# ==============================
def login(driver):
    try:
        print("Attempting cookie-based login...")
        # Try loading cookies
        try:
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            driver.get("https://www.linkedin.com")
            for cookie in cookies:
                driver.add_cookie(cookie)
            print("Cookies loaded successfully.")
            driver.refresh()
            time.sleep(3)
        except FileNotFoundError:
            print("⚠️ cookies.json not found, falling back to email/password login.")

        # Test if logged in
        driver.get("https://www.linkedin.com")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print(f"Page title: {driver.title}")

        if "log in" in driver.title.lower() or "login" in driver.current_url:
            print("❌ Cookie-based login failed or no valid cookies, attempting email/password login...")
            driver.get("https://www.linkedin.com/login")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            print(f"Login page title: {driver.title}")

            # Fill email
            email_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#session_key, input[name='session_key'], input#username"))
            )
            email_input.clear()
            email_input.send_keys(EMAIL)
            print("Email field filled.")

            # Fill password
            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#session_password, input[name='session_password'], input#password"))
            )
            password_input.clear()
            password_input.send_keys(PASSWORD)
            print("Password field filled.")

            # Click login button
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-id='sign-in-form__submit-btn'], button.sign-in-form__submit-btn, button[aria-label='Sign in']"))
            )
            login_button.click()
            print("Login button clicked.")

            # Wait for navigation
            time.sleep(10)

            # Check for CAPTCHA or 2FA
            if any(x in driver.current_url for x in ["checkpoint", "verification", "challenge"]):
                print("⚠️ CAPTCHA or 2FA detected! Solve it manually in the browser window.")
                while any(x in driver.current_url for x in ["checkpoint", "verification", "challenge"]):
                    time.sleep(5)
                print("✅ Verification solved.")
            else:
                print("✅ Email/password login successful.")
        else:
            print("✅ Cookie-based login successful.")

    except Exception as e:
        print(f"❌ Login failed: {e}")
        raise

# ==============================
# SCRAPER FUNCTION
# ==============================
def scrape_profile(url, driver):
    try:
        driver.get(url)
        time.sleep(random.randint(3, 6))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        name, job_title, company, location = "", "", "", ""
        # Name
        try:
            name = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1.text-heading-xlarge"))
            ).text
        except:
            pass

        # Job Title
        try:
            job_title = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.text-body-medium.break-words"))
            ).text
        except:
            pass

        # Location
        try:
            location = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "span.text-body-small.inline.t-black--light.break-words"))
            ).text
        except:
            pass

        # Company
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "section#experience-section"))
            )
            company_elements = driver.find_elements(By.CSS_SELECTOR, "div.pv-entity__company-summary-info h3 span:nth-child(2), ul.pv-profile-section__section-info li h3 span.pv-entity__secondary-title, section#experience li div.pv-entity__summary-info h3")
            company = company_elements[0].text if company_elements else ""
        except:
            pass

        name = name.strip()
        if name.lower() in ["join linkedin", "sign in"]:
            name = ""
        job_title = job_title.strip()
        company = company.strip()
        location = location.strip()

        print(f"✅ Scraped: {url}")
        return {
            "Linkedin_Link": url,
            "name": name,
            "job_title": job_title,
            "company": company,
            "location": location
        }
    except Exception as e:
        print(f"❌ Failed to scrape {url}: {e}")
        return {"Linkedin_Link": url, "name": "", "job_title": "", "company": "", "location": ""}

# ==============================
# MAIN
# ==============================
def main():
    print(f"Debug: Email={EMAIL}, Password={'*' * len(PASSWORD) if PASSWORD else 'None'}")
    df = pd.read_csv(INPUT_FILE)
    urls = df["Linkedin_Link"].dropna().apply(normalize_link).dropna().tolist()[:20]
    results = []

    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_window_size(1920, 1080)

    try:
        login(driver)
        for batch_start in range(0, len(urls), BATCH_SIZE):
            batch = urls[batch_start:batch_start + BATCH_SIZE]
            print(f"\n🚀 Starting batch {batch_start//BATCH_SIZE + 1}: {batch}")
            for url in batch:
                data = scrape_profile(url, driver)
                results.append(data)
                wait_time = random.randint(*DELAY_BETWEEN_PROFILES)
                print(f"⏳ Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            print("😴 Cooling down for 2 minutes...")
            time.sleep(120)
    finally:
        driver.quit()

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Saved results to {OUTPUT_FILE}")

# ==============================
# RUN
# ==============================
if __name__ == "__main__":
    main()