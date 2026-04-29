import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import altair as alt

    return alt, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Pracujeme zde s daty z ČSFD.
    """)
    return


@app.cell
def _(pl):
    df = pl.read_parquet("data/filmy.parquet").with_columns(
        pl.col("format")
        .fill_null("film")
        .str.replace_many(
            ["(seriál)", "(epizoda)", "(TV film)"], ["seriál", "seriál", "TV film"]
        )
    )

    lidi = pl.read_parquet("data/lidi.parquet")

    dokumenty = (
        df.explode("zanry")
        .filter(pl.col("zanry") == "dokumentární")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )
    return df, lidi


@app.cell
def _(df, pl):
    spoluherectvo = df.filter(pl.col("format").str.contains("film")).explode("hrají_id").select(pl.col("hrají_id")).to_series().to_list()
    return (spoluherectvo,)


@app.cell
def _(df, pl):
    def zebricek(sloupec):
        return (
            df.filter(format="film")
            .explode(sloupec)
            .filter(~pl.col(sloupec).is_null())
            .group_by(sloupec)
            .len()
            .sort(by="len", descending=True)
        )

    return (zebricek,)


@app.cell
def _(zebricek):
    zebricek("hrají")
    return


@app.cell
def _(zebricek):
    zebricek("režie")
    return


@app.cell
def _(lidi, pl, spoluherectvo):
    lidi.filter(pl.col("soubor").is_in(spoluherectvo)).select(pl.col("nar")).median()
    return


@app.cell
def _():
    return


@app.cell
def _(lidi, pl):
    narozeni = lidi.group_by(pl.col("nar").dt.year()).agg(pl.col("jmeno")).with_columns(pl.col("jmeno").list.join(", ")).rename({"nar":"rok"})
    zemreli = lidi.group_by(pl.col("zem").dt.year()).agg(pl.col("jmeno")).with_columns(pl.col("jmeno").list.join(", ")).rename({"zem":"rok"})
    return narozeni, zemreli


@app.cell
def _(lidi, narozeni, pl, spoluherectvo, zemreli):
    do_grafu = pl.concat(
        [
            lidi.filter(pl.col("soubor").is_in(spoluherectvo))
            .group_by(pl.col("nar").dt.year())
            .len()
            .with_columns(pl.lit("narození").alias("co"))
            .rename({"nar": "rok"}).join(narozeni,on="rok",how="left"),
            lidi.filter(pl.col("soubor").is_in(spoluherectvo))
            .group_by(pl.col("zem").dt.year())
            .len()
            .with_columns(pl.lit("úmrtí").alias("co"))
            .rename({"zem": "rok"}).join(zemreli, how="left", on="rok"),
        ]
    ).rename(
        {'len':'kolik'}
    ).drop_nulls()
    return (do_grafu,)


@app.cell
def _(do_grafu, pl):
    do_grafu.select(pl.col('kolik')).max()
    return


@app.cell
def _(do_grafu, pl):
    do_grafu.filter(pl.col("co") == "narození").filter(pl.col('rok') <= 1900).select(pl.col('kolik')).sum()
    return


@app.cell
def _(do_grafu, pl):
    do_grafu.filter(pl.col("co") == "narození").select(pl.col('rok')).min()
    return


@app.cell
def _(do_grafu, pl):
    do_grafu.filter(pl.col("co") == "narození").select(pl.col('rok')).max()
    return


@app.cell
def _():
    2010 - 1866
    return


@app.cell
def _(alt, do_grafu, pl):
    alt.Chart(
        do_grafu.filter(pl.col('co') == 'narození'), width=800
    ).mark_bar().encode(alt.X("rok:Q"), alt.Y("kolik:Q"))
    return


@app.cell
def _(alt, do_grafu, pl):
    alt.Chart(
        do_grafu.filter(pl.col('co') == 'úmrtí'), width=800
    ).mark_bar().encode(alt.X("rok:Q"), alt.Y("kolik:Q"))
    return


@app.cell
def _(do_grafu):
    do_grafu.sample(10)
    return


@app.cell
def _(do_grafu):
    do_grafu.write_json("data/narozeni-umrti.json")
    return


if __name__ == "__main__":
    app.run()
