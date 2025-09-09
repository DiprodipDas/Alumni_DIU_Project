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
EMAIL = os.getenv("FACEBOOK_EMAIL")
PASSWORD = os.getenv("FACEBOOK_PASSWORD")

# ==============================
# CONFIG
# ==============================
INPUT_FILE = "onlytenfb.csv"  # must have column "Facebook_Link"
OUTPUT_FILE = "facebook_profiles.csv"
BATCH_SIZE = 5  # profiles per run
DELAY_BETWEEN_PROFILES = (30, 90)  # seconds (min, max)
# ==============================
# HELPERS
# ==============================
def normalize_link(url: str) -> str:
    if not isinstance(url, str) or "facebook.com" not in url:
        return None
    parsed = urlparse(url)
    path = parsed.path
    return f"https://www.facebook.com{path}".split("?")[0]

# ==============================
# LOGIN FUNCTION
# ==============================
def login(driver):
    try:
        print("Attempting cookie-based login...")
        try:
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            driver.get("https://www.facebook.com")
            for cookie in cookies:
                driver.add_cookie(cookie)
            print("Cookies loaded successfully.")
            driver.refresh()
            time.sleep(3)
        except FileNotFoundError:
            print("⚠️ cookies.json not found, falling back to email/password login.")

        driver.get("https://www.facebook.com")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print(f"Page title: {driver.title}")

        if "log in" in driver.title.lower() or "login" in driver.current_url:
            print("❌ Cookie-based login failed or no valid cookies, attempting email/password login...")
            driver.get("https://www.facebook.com/login")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            print(f"Login page title: {driver.title}")

            # Fill email
            email_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#email, input[name='email']"))
            )
            email_input.clear()
            email_input.send_keys(EMAIL)
            print("Email field filled.")

            # Fill password
            password_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input#pass, input[name='pass']"))
            )
            password_input.clear()
            password_input.send_keys(PASSWORD)
            print("Password field filled.")

            # Click login button
            login_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[name='login'], button[data-testid='royal_login_button']"))
            )
            login_button.click()
            print("Login button clicked.")

            # Wait for potential CAPTCHA or 2FA
            time.sleep(10)

            # Check for CAPTCHA or 2FA
            if any(x in driver.current_url for x in ["checkpoint", "login/alert", "twofactor"]):
                print("⚠️ CAPTCHA or 2FA detected! Please solve it manually in the browser window.")
                print("After solving, press Enter in the console to continue...")
                input("Waiting for your confirmation (press Enter after solving CAPTCHA/2FA): ")

                # Verify login success
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                if any(x in driver.current_url for x in ["checkpoint", "login/alert", "twofactor"]):
                    print("❌ Still on CAPTCHA/2FA page. Please ensure you solved it correctly.")
                    raise Exception("CAPTCHA/2FA not resolved")
                else:
                    print("✅ CAPTCHA/2FA solved. Login successful.")
            else:
                print("✅ Email/password login successful.")
        else:
            print("✅ Cookie-based login successful.")

        # Final login verification
        driver.get("https://www.facebook.com")
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        if "log in" in driver.title.lower() or "login" in driver.current_url:
            print("❌ Login verification failed. Still on login page.")
            raise Exception("Failed to log in after CAPTCHA/2FA")
        print("✅ Final login verification successful.")

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
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

        name, job_title, company, location = "", "", "", ""
        # Name
        try:
            name = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
            ).text
        except:
            print(f"⚠️ Name not found for {url}")

        # Navigate to About page
        driver.get(f"{url}/about")
        time.sleep(random.randint(3, 6))
        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

        # Job Title and Company (from Work section)
        try:
            work_section = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-gt*='work']"))
            )
            print(f"Debug: Work section HTML for {url}: {work_section.get_attribute('outerHTML')[:500]}...")
            work_info = work_section.text.split("\n")
            job_title = work_info[0] if work_info else ""
            company = work_info[1] if len(work_info) > 1 else ""
        except:
            print(f"⚠️ Work section not found for {url}")
            driver.get(f"{url}/about_work_and_education")
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            try:
                work_elements = driver.find_elements(By.CSS_SELECTOR, "div.x1yztbdb div.x1i10hfl span")
                if work_elements:
                    job_title = work_elements[0].text if len(work_elements) > 0 else ""
                    company = work_elements[1].text if len(work_elements) > 1 else ""
            except:
                print(f"⚠️ Company/Job not found in Work tab for {url}")

        # Location (from Places section)
        try:
            places_section = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-gt*='places']"))
            )
            location = places_section.text.split("\n")[0]
        except:
            print(f"⚠️ Places section not found for {url}")
            driver.get(f"{url}/about_places")
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            try:
                location_elements = driver.find_elements(By.CSS_SELECTOR, "div.x1yztbdb div.x1i10hfl span")
                location = location_elements[0].text if location_elements else ""
            except:
                print(f"⚠️ Location not found in Places tab for {url}")

        name = name.strip()
        if name.lower() in ["facebook", "log in"]:
            name = ""
        job_title = job_title.strip()
        company = company.strip()
        location = location.strip()

        print(f"✅ Scraped: {url} (Company: {company})")
        return {
            "Facebook_Link": url,
            "name": name,
            "job_title": job_title,
            "company": company,
            "location": location
        }
    except Exception as e:
        print(f"❌ Failed to scrape {url}: {e}")
        return {"Facebook_Link": url, "name": "", "job_title": "", "company": "", "location": ""}

# ==============================
# MAIN
# ==============================
def main():
    print(f"Debug: Email={EMAIL}, Password={'*' * len(PASSWORD) if PASSWORD else 'None'}")
    df = pd.read_csv(INPUT_FILE)
    urls = df["Facebook_Link"].dropna().apply(normalize_link).dropna().tolist()[:20]
    results = []

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