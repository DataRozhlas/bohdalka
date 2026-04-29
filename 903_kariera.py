import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars.selectors as cs
    import altair as alt

    return (pl,)


@app.cell
def _(pl):
    pl.read_parquet("data/filmy.parquet").filter(pl.col("format").is_in(['(epizoda)','(TV film)']))
    return


@app.cell
def _(pl):
    dokumentarni_csfd = (
        pl.read_parquet("data/filmy.parquet")
        .explode("zanry")
        .filter(pl.col("zanry") == "dokumentární")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )

    doplneni1 = (
        pl.read_parquet("data/filmy.parquet")
        .filter(pl.col("format").is_in(['(epizoda)','(TV film)']))
        .filter(~pl.col("nazev").is_in(dokumentarni_csfd))
        .with_columns(
            pl.lit(["x","Jiřina Bohdalová (2878)"]).alias("Hrají"),
            pl.lit(["hraný"]).alias("Typologie")
        )
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
    )

    sloupce_csfd = doplneni1.columns

    doplneni2 = (
        pl.read_parquet("data/filmy.parquet")
        .filter(~pl.col("nazev").is_in(dokumentarni_csfd))
        .filter(pl.col("rok") >= 2017)
        .with_columns(
            pl.lit(["x","Jiřina Bohdalová (2878)"]).alias("Hrají"),
            pl.lit(["hraný"]).alias("Typologie")
        )
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
    )

    s_bohdalkou = (
        pl.read_parquet("../filmovy-prehled/data/filmy.parquet").explode("Hrají")
        .filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)")
        .explode("Typologie")
        .filter(pl.col("Typologie") == "hraný")
        .select(pl.col("Film"))
        .to_series()
        .to_list()
    )

    df = pl.concat(
        [
            pl.read_parquet("../filmovy-prehled/data/filmy.parquet").filter(pl.col("Film").is_in(s_bohdalkou)).with_columns(pl.col("Film").str.split(" (").list.slice(0,1)).explode("Film").sort(
                by="Copyright", descending=True
            ),
            doplneni1,
            doplneni2
        ],
        how="diagonal_relaxed",
    )
    return df, sloupce_csfd


@app.cell
def _(df):
    df.sort(by="Film")
    return


@app.cell
def _(c, df, pl):
    filmove_role = (
        df.filter(
            pl.col("Země původu").is_not_null()
            | (pl.col("Film") == "Vánoční příběh")
        )
        .with_columns(
            (
                1
                + pl.col("Hrají")
                .list.eval(
                    pl.arg_where(pl.element() == pl.lit("Jiřina Bohdalová (2878)"))
                )
                .list.first()
            ).alias("kolikata_bohdalka")
        )
        .select(pl.col(["Copyright", "Film", "kolikata_bohdalka"]))
        .sort(by="Copyright")
        .with_columns(pl.lit("film").alias("typ"))
    ).with_columns(
        pl.when(pl.col("kolikata_bohdalka") <= 3).then(pl.lit(True)).otherwise(pl.lit(False)).alias("hlavni")
    ).rename({'Copyright':'rok','Film':'film'}).drop('kolikata_bohdalka')

    c
    return (filmove_role,)


@app.cell
def _(df, pl, sloupce_csfd):
    tv_role = (
        df.filter(pl.col("Země původu").is_null())
        .select(pl.col(sloupce_csfd))
        .rename({"Film": "film", "Copyright": "rok"})
        .with_columns(pl.lit(None).alias("hlavni"))
        .with_columns(
            pl.when(pl.col("epizoda").is_not_null())
            .then(pl.col("film") + pl.lit(": ") + pl.col("epizoda"))
            .otherwise(pl.col("film"))
            .alias("film")
        )
        .with_columns(pl.lit("TV").alias("typ"))
        .select(pl.col(["rok", "film", "typ", "hlavni"]))
    )

    tv_role
    return (tv_role,)


@app.cell
def _(filmove_role, pl, tv_role):
    role = pl.concat([filmove_role, tv_role])
    return (role,)


@app.cell
def _(role):
    role.write_json("data/role.json")
    return


if __name__ == "__main__":
    app.run()
