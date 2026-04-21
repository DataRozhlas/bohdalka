import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import asyncio
    import threading
    from playwright.async_api import async_playwright, TimeoutError
    from bs4 import BeautifulSoup
    import sys

    return (
        BeautifulSoup,
        TimeoutError,
        async_playwright,
        asyncio,
        sys,
        threading,
    )


@app.cell
def _(asyncio, sys):
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    return


@app.cell
def _(BeautifulSoup, TimeoutError, async_playwright, asyncio, threading):
    def run_in_proactor(coro):
        """Runs an async function in a separate thread with a ProactorEventLoop."""
        result = []
        def wrapper():
            # 1. Force the Proactor loop for this specific thread
            loop = asyncio.WindowsProactorEventLoopPolicy().new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # 2. Run the code and capture the result
                result.append(loop.run_until_complete(coro))
            finally:
                loop.close()

        thread = threading.Thread(target=wrapper)
        thread.start()
        thread.join()
        return result[0]

    try:
        from playwright_stealth import stealth_async
        HAS_STEALTH = True
    except ImportError:
        HAS_STEALTH = False

    async def scrape_logic(url):
        async with async_playwright() as p:
            # Launching with a visible window to bypass bot detection
            browser = await p.chromium.launch(headless=False) 
        
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                locale="cs-CZ",
                viewport={'width': 1280, 'height': 720}
            )
        
            page = await context.new_page()

            if HAS_STEALTH:
                await stealth_async(page)
            else:
                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            print(f"Opening: {url}")
        
            try:
                # We attempt to wait for 'networkidle', but set a timeout (e.g., 30 or 60 seconds)
                # If the site keeps loading resources, the script will trigger the 'except' block
                await page.goto(url, wait_until="networkidle", timeout=20000)
            except TimeoutError:
                print(f"Navigation timeout reached for {url}. Capturing partial page content...")
            except Exception as e:
                print(f"An unexpected error occurred during navigation: {e}")

            # --- CRITICAL STEP FOR CSFD / BOT CHECKS ---
            # Even if a timeout occurred, we still wait for the manual check or late-loading elements
            print("Waiting 5 seconds for anti-bot check/settling...")
            await asyncio.sleep(5) 
        
            # Get the content regardless of whether the goto finished successfully or timed out
            content = await page.content()
            await browser.close()
        
            # Parse what we managed to get
            soup = BeautifulSoup(content, 'html.parser')
        
            return soup

    return run_in_proactor, scrape_logic, stealth_async


@app.cell
def _(async_playwright, asyncio, os, stealth_async):
    async def scrape_batch_logic(urls, output_dir):
        async with async_playwright() as p:
            # Launch once
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await stealth_async(page) # or manual humanizer

            results = []
            for url in urls:
                # Create filename from URL
                filename = url.split('/')[-3] + ".html"
                filepath = os.path.join(output_dir, filename)

                # Skip if already downloaded
                if os.path.exists(filepath):
                    print(f"Skipping (exists): {filename}")
                    continue

                print(f"Processing: {filename}")
            
                try:
                    # Use 'domcontentloaded' - it's much faster and less likely to timeout
                    await page.goto(url, wait_until="domcontentloaded", timeout=120000)
                
                    # Small human-like pause
                    await asyncio.sleep(2) 
                
                    content = await page.content()
                
                    with open(filepath, 'w+', encoding='utf-8') as f:
                        f.write(content)
                
                    results.append(filename)

                except Exception as e:
                    print(f"Failed to download {url}: {e}")
                    # We don't 'raise' here, so the loop continues to the next film
                    continue
        
            await browser.close()
            return results

    return


@app.cell
def _(run_in_proactor, scrape_logic):
    url = "https://www.csfd.cz/tvurce/1088-jirina-bohdalova/prehled/"
    # Instead of 'await scrape_logic', use our wrapper:
    bohdalova = run_in_proactor(scrape_logic(url))
    return (bohdalova,)


@app.cell
def _(bohdalova):
    filmy1 = [x.get('href') for x in bohdalova.find_all("a")]
    start_link = "/film/644941-bubenik-a-princezna/prehled/"
    end_link = "/film/242130-jak-se-chovati/prehled/"

    try:
        # 1. Find the index of the starting link and add 1 (to start AFTER it)
        start_idx = filmy1.index(start_link)
    
        # 2. Find the index of the ending link
        end_idx = filmy1.index(end_link) + 1

        # 3. Slice the list to keep only the middle section
        filmy_filtered = filmy1[start_idx:end_idx]

        print(f"Successfully extracted {len(filmy_filtered)} links.")

    except ValueError:
        print("One of the marker links was not found in the filmy1 list.")
    filmy2 = [x for x in filmy_filtered if x]
    filmy3 = [x for x in filmy2 if '/film/' in x]
    filmy3
    return (filmy3,)


@app.cell
def _():
    import os

    return (os,)


@app.cell
def _(filmy3):
    filmy_nechceme = []
    for f3 in filmy3:
        print(f3)
        for f4 in filmy3:
            if f3.replace('prehled/','') in f4 and f3 != f4:
                filmy_nechceme.append(f3)
    return (filmy_nechceme,)


@app.cell
def _(filmy3, filmy_nechceme):
    filmy4 = list(set([f for f in filmy3 if f not in filmy_nechceme]))
    filmy4
    return (filmy4,)


@app.cell
def _(os):
    kam = 'downloads/filmy'
    os.makedirs(kam,exist_ok=True)
    return (kam,)


@app.cell
def _(filmy4, kam, os, run_in_proactor, scrape_logic):
    for f in filmy4:
        url_film = 'https://www.csfd.cz' + f
        soubor = f.split('/')[-3] + ".html"
        if soubor not in os.listdir(kam):
            print(soubor)
            film = run_in_proactor(scrape_logic(url_film))
            with open(os.path.join(kam,soubor),'w+',encoding='utf-8') as ukladame:
                ukladame.write(str(film))
    return


if __name__ == "__main__":
    app.run()
