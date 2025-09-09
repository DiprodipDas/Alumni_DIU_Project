import asyncio
import pandas as pd
import json
import random
import time
from urllib.parse import urlparse
from playwright.async_api import async_playwright

# ==============================
# CONFIG
# ==============================
INPUT_FILE = "onlytwentyen.csv"        # Must have column "linkedin_Link"
OUTPUT_FILE = "linkedin_cookie_scraped2.csv"
COOKIE_FILE = "cookies.json"             # Your li_at cookie file
BATCH_SIZE = 5                           # Profiles per run
DELAY_BETWEEN = (5, 12)                  # Random delay in seconds

# ==============================
# HELPERS
# ==============================
def normalize_link(url: str) -> str:
    """Normalize LinkedIn profile links to standard www.linkedin.com"""
    if not isinstance(url, str) or "linkedin.com" not in url:
        return None
    parsed = urlparse(url)
    path = parsed.path
    return f"https://www.linkedin.com{path}".split("?")[0]

async def load_cookies(context, cookie_file):
    """Load cookies into the browser context"""
    with open(cookie_file, "r") as f:
        cookies = json.load(f)
    await context.add_cookies(cookies)

# ==============================
# SCRAPER
# ==============================
async def scrape_profile(url, page):
    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(random.randint(3000, 6000))

        name, job_title, company, location = "", "", "", ""

        # Name
        if await page.locator("h1").count():
            name = await page.locator("h1").inner_text()

        # Job title (headline under name)
        if await page.locator("div.text-body-medium.break-words").count():
            job_title = await page.locator("div.text-body-medium.break-words").inner_text()

        # 🔹 Company (Experience section)
        try:
            exp_section = page.locator("section:has(h2:has-text('Experience'))")
            await exp_section.wait_for(timeout=10000)

            first_role = exp_section.locator("li").nth(0)

            # The company is usually the SECOND visible span inside the role
            spans = first_role.locator("span[aria-hidden='true']")
            if await spans.count() >= 2:
                company = await spans.nth(1).inner_text()
            else:
                # Fallback: search for company-specific classes
                alt = first_role.locator("span.t-14.t-normal.t-black")
                if await alt.count():
                    company = await alt.nth(0).inner_text()
        except:
            company = ""
        # Location
        if await page.locator("span.text-body-small.inline.t-black--light.break-words").count():
            location = await page.locator("span.text-body-small.inline.t-black--light.break-words").nth(0).inner_text()

        return {
            "Linkedin_Link": url,
            "name": name.strip(),
            "job_title": job_title.strip(),
            "company": company.strip(),
            "location": location.strip()
        }

    except Exception as e:
        print(f"❌ Failed to scrape {url}: {e}")
        return {"Linkedin_Link": url, "name": "", "job_title": "", "company": "", "location": ""}

# ==============================
# MAIN
# ==============================
async def main():
    df = pd.read_csv(INPUT_FILE)
    if "Linkedin_Link" not in df.columns:
        raise ValueError("❌ Input CSV must have a column named 'Linkedin_Link'")

    urls = df["Linkedin_Link"].dropna().apply(normalize_link).dropna().tolist()
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=False to debug
        context = await browser.new_context()

        # Load your li_at cookie
        await load_cookies(context, COOKIE_FILE)

        page = await context.new_page()

        for i, url in enumerate(urls, start=1):
            print(f"\n➡️ [{i}/{len(urls)}] Scraping: {url}")
            data = await scrape_profile(url, page)
            results.append(data)

            wait_time = random.randint(*DELAY_BETWEEN)
            print(f"⏳ Waiting {wait_time} seconds...")
            time.sleep(wait_time)

        await browser.close()

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Done! Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
