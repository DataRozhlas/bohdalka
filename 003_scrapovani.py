import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import os
    import re
    from bs4 import BeautifulSoup
    import polars as pl

    return BeautifulSoup, os, pl, re


@app.cell
def _(os):
    slozka_filmy = "downloads/filmy"
    filmy = os.listdir(slozka_filmy)
    return filmy, slozka_filmy


@app.cell
def _(BeautifulSoup, os, re, slozka_filmy):
    def oscrapuj_film(film):
        with open(
            os.path.join(slozka_filmy, film), "r", encoding="utf-8"
        ) as vstup:
            soup = BeautifulSoup(vstup.read(), "html.parser")
            slovnik = {}
            slovnik["soubor"] = film
            if soup.find(class_="film-series-content"):
                slovnik["nazev"] = (
                    soup.find(class_="film-series-content").find("a").text.strip()
                )
                slovnik["epizoda"] = soup.find("h1").text.strip()
            else:
                slovnik["nazev"] = soup.find("h1").text.strip()
                slovnik["epizoda"] = None
            if soup.find(class_="type"):
                slovnik["format"] = soup.find(class_="type").text
            slovnik["rok"] = int(
                re.search(r"\d{4}", soup.find("title").text).group()
            )
            if soup.find(class_="plot-full hidden"):
                slovnik["plot"] = soup.find(class_="plot-full hidden").text.strip()
            slovnik["zanry"] = [
                a.text.strip().lower()
                for a in soup.find(class_="genres").find_all("a")
            ]
            slovnik["zeme"] = [
                x.strip()
                for x in soup.find(class_="origin").text.splitlines()[0].split("/")
            ]
            try:
                slovnik["stopaz"] = int(
                    soup.find(class_="origin").text.split(" min")[0].split(" ")[-1]
                )
            except:
                print(f"{film}: chybí stopáž")
            try:
                slovnik["hodnoceni"] = int(
                    soup.find(class_="film-rating-average")
                    .text.replace("%", "")
                    .strip()
                )
            except:
                pass
            try:
                slovnik["hodnoceni_n"] = int(
                    re.sub(
                        r"[^\d]",
                        "",
                        soup.find(class_="more more-modal-ratings-fanclub")
                        .find("strong")
                        .text.strip(),
                    )
                )
            except:
                pass
            try:
                slovnik["tagy"] = [
                    x.text.strip()
                    for x in soup.find(class_="updated-box-content-tags").find_all(
                        "a"
                    )
                ]
            except:
                pass
            for div in soup.find_all("div"):
                if div.find("h4"):
                    slovnik[
                        div.find("h4").text.strip().lower().replace(":", "")
                    ] = [x.text.strip() for x in div.find_all("a")]
                    slovnik[
                        div.find("h4").text.strip().lower().replace(":", "")
                        + "_id"
                    ] = [
                        x.get("href")
                        .replace("/tvurce/", "")
                        .replace("/prehled/", "")
                        for x in div.find_all("a")
                    ]
            return slovnik

    return (oscrapuj_film,)


@app.cell
def _(filmy, oscrapuj_film):
    seznam_filmu = []
    for f in filmy:
        try:
            novy_film = oscrapuj_film(f)
            seznam_filmu.append(novy_film)
        except Exception as e:
            print(e)
            print(f)
    return (seznam_filmu,)


@app.cell
def _(os):
    os.makedirs("data", exist_ok=True)
    return


@app.cell
def _(pl, seznam_filmu):
    df_filmy = pl.DataFrame(seznam_filmu)
    df_filmy.write_parquet("data/filmy.parquet")
    return (df_filmy,)


@app.cell
def _(df_filmy):
    df_filmy
    return


@app.cell
def _(os):
    slozka_lidi = "downloads/herectvo"
    lidi = os.listdir(slozka_lidi)
    return lidi, slozka_lidi


@app.cell
def _(BeautifulSoup, os, re, slozka_lidi):
    def oscrapuj_cloveka(clovek):
        with open(
            os.path.join(slozka_lidi, clovek), "r", encoding="utf-8"
        ) as vstup:
            soup = BeautifulSoup(vstup.read(), "html.parser")
            slovnik = {}
            slovnik["soubor"] = clovek.split(".")[0]
            try:
                slovnik["jmeno"] = soup.find("h1").text.strip()
            except:
                return None
            try:
                detaily = soup.find(class_="creator-profile-details").text
            except:
                detaily = None
            if detaily:
                try:
                    slovnik["nar"] = (
                        re.search(r"nar. \d{2}\.\d{2}\.\d{4}", detaily)
                        .group()
                        .split(" ")[-1]
                    )
                except:
                    pass
                try:
                    slovnik["zem"] = (
                        re.search(r"zem. \d{2}\.\d{2}\.\d{4}", detaily)
                        .group()
                        .split(" ")[-1]
                    )
                except:
                    pass
            try:
                slovnik["fanklub"] = int(
                    soup.find(class_="fans-box-mobile-content")
                    .find("strong")
                    .text.strip()
                )
            except:
                pass
        return slovnik

    return (oscrapuj_cloveka,)


@app.cell
def _(oscrapuj_cloveka):
    oscrapuj_cloveka("1729-ladislav-chudik.html")
    return


@app.cell
def _(lidi, oscrapuj_cloveka):
    seznam_lidi = []
    for l in lidi:
        novy_clovek = oscrapuj_cloveka(l)
        seznam_lidi.append(novy_clovek)
    return (seznam_lidi,)


@app.cell
def _(pl, seznam_lidi):
    df_lidi = (
        pl.DataFrame(seznam_lidi)
        .filter(pl.col("soubor").str.len_bytes() > 0)
        .with_columns(pl.col("nar").str.to_date(), pl.col("zem").str.to_date())
    ).filter(pl.col("soubor").str.contains("-"))
    return (df_lidi,)


@app.cell
def _(df_lidi):
    df_lidi
    return


@app.cell
def _(df_lidi):
    df_lidi.write_parquet("data/lidi.parquet")
    return


@app.cell
def _(df_lidi, pl):
    oprava = [
        x + ".html"
        for x in df_lidi.filter(pl.col("jmeno") == "Tento web není dostupný")
        .select(pl.col("soubor"))
        .to_series()
        .to_list()
    ]
    oprava
    return (oprava,)


@app.cell
def _(oprava, os, slozka_lidi):
    for o in oprava:
        print(f"mažu {o}")
        try:
            os.remove(os.path.join(slozka_lidi, o))
        except:
            pass
    return


if __name__ == "__main__":
    app.run()
