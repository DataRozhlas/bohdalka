import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import altair as alt

    return alt, pl


@app.cell
def _(pl):
    df = pl.read_parquet("data/filmy.parquet").with_columns(
        pl.col('format').fill_null('film').str.replace_many(['(seriál)','(epizoda)','(TV film)'],['seriál','seriál','TV film'])
    )
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(df):
    df.columns
    return


@app.cell
def _(alt, df):
    alt.Chart(
        df.sort(by='rok').unique(subset=['nazev'],keep='first').group_by(["rok",'format']).len().sort(by="rok"),
        width=800
    ).mark_bar(
    ).encode(
        alt.X("rok:Q"),
        alt.Y("len:Q"),
        alt.Color('format:N')
    )
    return


@app.cell
def _(df, pl):
    dokumenty = df.explode('zanry').filter(pl.col('zanry') == 'dokumentární').select(pl.col('nazev')).to_series().to_list()
    return (dokumenty,)


@app.cell
def _(alt, df, dokumenty, pl):
    alt.Chart(
        df.filter((pl.col('hodnoceni') > 0) & (pl.col('format') == 'film') & (~pl.col('nazev').is_in(dokumenty))).group_by("hodnoceni").len(),
        width=800
    ).mark_bar(
    ).encode(
        alt.X("hodnoceni:Q"),
        alt.Y("len:Q")
    )
    return


@app.cell
def _(df, pl):
    df.sort(by="hodnoceni").filter(pl.col("hodnoceni") > 0).head(5)
    return


@app.cell
def _(df, pl):
    df.sort(by="hodnoceni",descending=True).filter(pl.col('hodnoceni') > 0).head(5)
    return


@app.cell
def _(df, dokumenty, pl):
    df.sort(by="hodnoceni",descending=True).filter((pl.col('hodnoceni') > 0) & (pl.col('format') == 'film') & (~pl.col('nazev').is_in(dokumenty))).head(10)
    return


@app.cell
def _(df, pl):
    def zebricek(sloupec):
        return df.filter(format="film").explode(sloupec).filter(~pl.col(sloupec).is_null()).group_by(sloupec).len().sort(by="len",descending=True)

    return (zebricek,)


@app.cell
def _():
    return


@app.cell
def _(zebricek):
    zebricek("hrají")
    return


@app.cell
def _(zebricek):
    zebricek("režie")
    return


@app.cell
def _(zebricek):
    zebricek("zeme")
    return


@app.cell
def _(zebricek):
    zebricek("zanry")
    return


@app.cell
def _(zebricek):
    zebricek("tagy")
    return


@app.cell
def _(pl):
    lidi = pl.read_parquet("data/lidi.parquet")
    return (lidi,)


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("nar").is_null()).sort(by="nar").head(100)
    return


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("nar").is_null()).sort(by="nar").filter(pl.col("nar").dt.year() <= 1900)
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("hrají").list.contains("Jiřina Bohdalová") & (pl.col('hrají').list.contains("Bohdan Lachman")))
    return


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("nar").is_null()).sort(by="nar",descending=True).head(100)
    return


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("nar").is_null()).sort(by="nar",descending=True).filter(pl.col("nar").dt.year() >= 2001)
    return


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("zem").is_null()).sort(by="zem").head(100)
    return


@app.cell
def _(lidi, pl):
    lidi.filter(~pl.col("zem").is_null()).sort(by="zem",descending=True).head(100)
    return


@app.cell
def _(alt, lidi, pl):
    alt.Chart(
        lidi.group_by(pl.col("nar").dt.year()).len(), width=800
    ).mark_bar().encode(alt.X("nar:Q"), alt.Y("len:Q"))
    return


@app.cell
def _(alt, lidi, pl):
    alt.Chart(
        lidi.group_by(pl.col("zem").dt.year()).len(), width=800
    ).mark_bar().encode(alt.X("zem:Q"), alt.Y("len:Q"))
    return


@app.cell
def _(pl):
    dokumentarni = (
        pl.read_parquet("data/filmy.parquet")
        .explode("zanry")
        .filter(pl.col("zanry") == "dokumentární")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )
    return (dokumentarni,)


@app.cell
def _(dokumentarni, pl):
    pl.read_parquet("data/filmy.parquet").filter(pl.col("format").is_null()).filter(~pl.col("nazev").is_in(dokumentarni)).with_columns(pl.lit(["Jiřina Bohdalová (2878)"]).alias("Hrají")).filter(pl.col("rok") > 2016).rename({"rok": "Copyright", "nazev": "Film","stopaz":"Minutáž"})
    return


if __name__ == "__main__":
    app.run()
