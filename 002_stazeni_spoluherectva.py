import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
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
        os,
        sys,
        threading,
    )


@app.cell
def _(asyncio, sys):
    if sys.platform == "win32":
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
                viewport={"width": 1280, "height": 720},
            )

            page = await context.new_page()

            if HAS_STEALTH:
                await stealth_async(page)
            else:
                await page.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )

            print(f"Opening: {url}")

            try:
                # We attempt to wait for 'networkidle', but set a timeout (e.g., 30 or 60 seconds)
                # If the site keeps loading resources, the script will trigger the 'except' block
                await page.goto(url, wait_until="networkidle", timeout=20000)
            except TimeoutError:
                print(
                    f"Navigation timeout reached for {url}. Capturing partial page content..."
                )
            except Exception as e:
                print(f"An unexpected error occurred during navigation: {e}")

            # --- CRITICAL STEP FOR CSFD / BOT CHECKS ---
            # Even if a timeout occurred, we still wait for the manual check or late-loading elements
            print("Waiting 5 seconds for anti-bot check/settling...")
            await asyncio.sleep(2)

            # Get the content regardless of whether the goto finished successfully or timed out
            content = await page.content()
            await browser.close()

            # Parse what we managed to get
            soup = BeautifulSoup(content, "html.parser")

            return soup

    return run_in_proactor, scrape_logic


@app.cell
def _():
    odkud = "downloads/filmy"
    return (odkud,)


@app.cell
def _(BeautifulSoup, odkud, os):
    def najdi_herectvo(soubor):
        with open(os.path.join(odkud, soubor), "r", encoding="utf-8") as pokus:
            soup = BeautifulSoup(pokus.read(), features="html.parser")
            for div in soup.find_all("div"):
                if div.find("h4"):
                    if div.find("h4").text == "Hrají:":
                        return [x.get("href") for x in div.find_all("a")]

    return (najdi_herectvo,)


@app.cell
def _(najdi_herectvo, odkud, os):
    herectvo = []
    for x in os.listdir(odkud):
        print(x)
        dalsi_herectvo = najdi_herectvo(x)
        if dalsi_herectvo:
            herectvo += dalsi_herectvo
    vsechno_herectvo = list(set(herectvo))
    return (vsechno_herectvo,)


@app.cell
def _(vsechno_herectvo):
    len(vsechno_herectvo)
    return


@app.cell
def _(vsechno_herectvo):
    vsechno_herectvo
    return


@app.cell
def _(os):
    kam = "downloads/herectvo"
    os.makedirs(kam, exist_ok=True)
    return (kam,)


@app.cell
def _(kam, os, run_in_proactor, scrape_logic, vsechno_herectvo):
    for h in vsechno_herectvo:
        url_herectvo = "https://www.csfd.cz" + h
        try:
            soubor = h.split("/")[-3] + ".html"
            if soubor not in os.listdir(kam):
                print(soubor)
                film = run_in_proactor(scrape_logic(url_herectvo))
                with open(
                    os.path.join(kam, soubor), "w+", encoding="utf-8"
                ) as ukladame:
                    ukladame.write(str(film))
        except Exception as e:
            print(h)
            print(e)
    return


if __name__ == "__main__":
    app.run()
