import asyncio
import os
import pandas as pd
import random
import re
from playwright.async_api import async_playwright
from dotenv import load_dotenv

load_dotenv()

# ==============================
# CONFIG
# ==============================
OUTPUT_FILE = "apostille_data_extra_9.csv"
LOGIN_URL = "https://apostille.mygov.bd/login"
BASE_DASHBOARD_URL = "https://apostille.mygov.bd/front-desk/dashboard?applicationStatus=completed&page="
BASE_APPLICATION_URL = "https://apostille.mygov.bd/applications/"

USERNAME = os.getenv("APOSTILLE_USERNAME")
PASSWORD = os.getenv("APOSTILLE_PASSWORD")

# ================== CHANGE THESE ==================
START_PAGE = 1      # Start from this page
END_PAGE = 1        # End at this page
# =================================================

DELAY_BETWEEN_APPS = (4, 8)

# ==============================
# HELPERS
# ==============================
async def login(page):
    try:
        print("🔐 Logging in...")
        await page.goto(LOGIN_URL, timeout=60000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        await page.fill('input[name="username"], input[id*="username"]', USERNAME)
        await page.fill('input[name="password"], input[id*="password"]', PASSWORD)
        await page.click('button[type="submit"], button:has-text("লগইন")')
        
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(5000)
        print("✅ Login successful!")
        return True
    except Exception as e:
        print(f"❌ Login error: {e}")
        return False


async def get_application_ids_from_range(page, start_page, end_page):
    all_ids = []
    print(f"📋 Collecting applications from Page {start_page} to {end_page}...")

    for p in range(start_page, end_page + 1):
        try:
            url = f"{BASE_DASHBOARD_URL}{p}"
            print(f"  → Page {p}")
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            links = await page.query_selector_all('a[href*="/applications/"]')
            page_ids = []
            for link in links:
                href = await link.get_attribute('href')
                if href:
                    app_id = href.split("/applications/")[-1].split("?")[0]
                    if app_id.isdigit():
                        page_ids.append(app_id)

            page_ids = list(dict.fromkeys(page_ids))
            all_ids.extend(page_ids)
            print(f"     Found {len(page_ids)} applications")
            
            await asyncio.sleep(random.uniform(2, 4))
            
        except Exception as e:
            print(f"  Error on page {p}: {e}")
            continue

    all_ids = list(dict.fromkeys(all_ids))
    print(f"\n✅ Total unique applications collected: {len(all_ids)}")
    return all_ids


# (Keep the same get_attachments and scrape_application functions as before)
async def get_attachments(page):
    try:
        imgs = await page.query_selector_all('img')
        urls = []
        for img in imgs:
            src = await img.get_attribute('src')
            if src and 'mygov.bd/storage/citizenv2' in src:
                urls.append(src)
        return '; '.join(urls)
    except:
        return ""


async def scrape_application(page, application_id):
    url = f"{BASE_APPLICATION_URL}{application_id}?applicationStatus=completed&page=1"
    try:
        await page.goto(url, timeout=90000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(4000)

        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
        await page.wait_for_timeout(2500)

        full_text = await page.evaluate('''() => {
            let text = document.body.innerText || '';
            const iframes = document.querySelectorAll('iframe');
            for (let iframe of iframes) {
                try {
                    if (iframe.contentDocument) {
                        text += '\\n\\n' + iframe.contentDocument.body.innerText;
                    }
                } catch(e) {}
            }
            return text;
        }''')

        data = {
            "application_id": application_id,
            "পাশের সন": "",
            "পরিচিতি নম্বর": "",
            "সিজিপিএ/জিপিএ/ডিভিশন": "",
            "আবেদনকারীর নাম (ইংরেজীতে)": "",
            "পিতার নাম": "",
            "মাতার নাম": "",
            "জন্ম তারিখ": "",
            "জাতীয় পরিচয়পত্র নম্বর": "",
            "মোবাইল নম্বর": "",
            "ই-মেইল": "",
            "সংযুক্তি সমূহ": await get_attachments(page),
            "Status": "নিষ্পত্তিকৃত"
        }

        # Parsing
        m = re.search(r'আবেদনকারীর নাম \(ইংরেজীতে\)\s*([^\n]+)', full_text)
        if m:
            data["আবেদনকারীর নাম (ইংরেজীতে)"] = m.group(1).strip()
        else:
            m = re.search(r'Applicant Name\s*[:：]?\s*([^\n]+)', full_text, re.IGNORECASE)
            if m:
                data["আবেদনকারীর নাম (ইংরেজীতে)"] = m.group(1).strip()

        m = re.search(r'পাশের সন\s*(\d{4})', full_text)
        if m: data["পাশের সন"] = m.group(1)

        m = re.search(r'পরিচিতি নম্বর লিখুন\s*([^\n]+)', full_text)
        if m: data["পরিচিতি নম্বর"] = m.group(1).strip()

        m = re.search(r'(সিজিপিএ|জিপিএ|ডিভিশন)\s*[:：]?\s*([\d.]+)', full_text)
        if m: data["সিজিপিএ/জিপিএ/ডিভিশন"] = m.group(2)

        m = re.search(r'পিতার নাম\s*([^\n]+)', full_text)
        if m: data["পিতার নাম"] = m.group(1).strip()

        m = re.search(r'মাতার নাম\s*([^\n]+)', full_text)
        if m: data["মাতার নাম"] = m.group(1).strip()

        m = re.search(r'জন্ম তারিখ\s*[:：]?\s*([\d-]+)', full_text)
        if m: data["জন্ম তারিখ"] = m.group(1)

        m = re.search(r'জাতীয় পরিচয়পত্র নম্বর\s*(\d+)', full_text)
        if m: data["জাতীয় পরিচয়পত্র নম্বর"] = m.group(1)

        m = re.search(r'মোবাইল নম্বর\s*(\d{11})', full_text)
        if m: data["মোবাইল নম্বর"] = m.group(1)

        m = re.search(r'ই-মেইল\s*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', full_text)
        if m: data["ই-মেইল"] = m.group(1)

        return data

    except Exception as e:
        print(f"❌ Error scraping {application_id}: {e}")
        return {"application_id": application_id, "Status": "নিষ্পত্তিকৃত", "error": str(e)}


# ==============================
# MAIN
# ==============================
async def main():
    print("=" * 90)
    print(f"🚀 Scraping Page {START_PAGE} to {END_PAGE}")
    print("=" * 90)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        if not await login(page):
            await browser.close()
            return

        application_ids = await get_application_ids_from_range(page, START_PAGE, END_PAGE)

        all_results = []
        total = len(application_ids)

        print(f"\n📊 Starting to scrape {total} applications...\n")

        for i, app_id in enumerate(application_ids, 1):
            print(f"📝 [{i:3d}/{total}] Scraping: {app_id}")
            data = await scrape_application(page, app_id)
            all_results.append(data)

            if i % 20 == 0:
                pd.DataFrame(all_results).to_csv("temp_progress.csv", index=False, encoding='utf-8-sig')
                print(f"💾 Progress saved ({i}/{total})")

            await asyncio.sleep(random.randint(*DELAY_BETWEEN_APPS))

        df = pd.DataFrame(all_results)
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        print(f"\n✅ DONE! Data saved to {OUTPUT_FILE}")
        print(f"Total applications scraped: {len(all_results)}")
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())