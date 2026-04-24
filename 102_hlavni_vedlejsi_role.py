import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars.selectors as cs
    import altair as alt

    return mo, pl


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

    doplneni = (
        pl.read_parquet("data/filmy.parquet")
        .filter(pl.col("format").is_null())
        .filter(~pl.col("nazev").is_in(dokumentarni))
        .with_columns(
            pl.lit(["x","Jiřina Bohdalová (2878)"]).alias("Hrají"),
            pl.lit(["hraný"]).alias("Typologie")
        )
        .filter(pl.col("rok") > 2016)
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
    )

    df = pl.concat(
        [
            pl.read_parquet("../filmovy-prehled/data/filmy.parquet").sort(
                by="Copyright", descending=True
            ),
            doplneni,
        ],
        how="diagonal_relaxed",
    )
    return (df,)


@app.cell
def _(df, pl):
    s_bohdalkou = (
        df.explode("Hrají")
        .filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)")
        .explode("Typologie")
        .filter(pl.col("Typologie") == "hraný")
        .select(pl.col("Film"))
        .to_series()
        .to_list()
    )
    return (s_bohdalkou,)


@app.cell
def _(s_bohdalkou):
    s_bohdalkou
    return


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)").sort(by="Copyright",descending=True)
    return


@app.cell
def _(df):
    df
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Celovečerní filmy JB s pořadím v kariéře a pořadím JB v distribučních materiálech
    """)
    return


@app.cell
def _(df, pl, s_bohdalkou):
    df_poradi = (
        df.filter(pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70))
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
        .with_row_index(offset=1)
    )

    df_poradi
    return (df_poradi,)


@app.cell
def _(df_poradi, pl):
    df_poradi.filter(pl.col("kolikata_bohdalka") <= 3)
    return


@app.cell
def _(df_poradi, pl):
    bohdalka_hlavni = (
        df_poradi.filter(pl.col("kolikata_bohdalka") <= 3)
        .select(pl.col("Film"))
        .to_series()
        .to_list()
    )
    bohdalka_hlavni
    return (bohdalka_hlavni,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Podíl hlavních rolí
    """)
    return


@app.cell
def _(df_poradi, pl):
    len(df_poradi.filter(pl.col("kolikata_bohdalka") <= 3)) / len(df_poradi)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Spoluherectvo
    """)
    return


@app.cell
def _(bohdalka_hlavni, df, pl, s_bohdalkou):
    df.filter(pl.col("Film").is_in(bohdalka_hlavni)).filter(
        pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)
    ).with_columns(pl.col("Hrají").list.slice(0, 3)).explode("Hrají").group_by(
        "Hrají"
    ).len().sort(by="len", descending=True)
    return


@app.cell
def _(bohdalka_hlavni, df, pl, s_bohdalkou):
    df.filter(pl.col("Film").is_in(bohdalka_hlavni)).filter(
        pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)
    ).with_columns(pl.col("Hrají").list.slice(0, 3)).explode("Hrají").filter(
        pl.col("Hrají") == "Radoslav Brzobohatý (2770)"
    ).select(pl.col(["Copyright", "Film"]))
    return


@app.cell
def _(df, pl, s_bohdalkou):
    df.filter(
        pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)
    ).with_columns(pl.col("Hrají").list.slice(0, 3)).explode("Hrají").group_by(
        "Hrají"
    ).len().sort(by="len", descending=True)
    return


@app.cell
def _(df, pl):
    df.filter(
        pl.col("Minutáž") > 70
    ).with_columns(pl.col("Hrají").list.slice(0,3)).explode("Hrají").group_by(
        "Hrají"
    ).len().sort(by="len", descending=True).with_row_index()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## S kolika lidmi hrála?
    """)
    return


@app.cell
def _(df, pl):
    len(df.filter(
        pl.col("Minutáž") > 70
    ).explode("Hrají").group_by(
        "Hrají"
    ).len())
    return


@app.cell
def _(df, pl, s_bohdalkou):
    len(df.filter(
        pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)
    ).explode("Hrají").group_by(
        "Hrají"
    ).len())
    return


@app.cell
def _(df, pl):
    nejobsazovanejsi = df.explode("Typologie").filter(pl.col("Typologie") == "hraný").filter(pl.col("Minutáž") > 70).explode("Hrají").group_by("Hrají").len().filter(pl.col("len") >= 50).sort(by="len",descending=True).to_series().to_list()
    return (nejobsazovanejsi,)


@app.cell
def _(df, nejobsazovanejsi, pl):
    predpriprava = df.explode("Typologie").filter(pl.col("Typologie") == "hraný").filter(pl.col("Minutáž") > 70).explode("Hrají")
    s_kolika = []
    for o in nejobsazovanejsi:
        filmy = predpriprava.filter(pl.col('Hrají') == o).select(pl.col("Film")).to_series().to_list()
        spoluherectvo = predpriprava.filter(pl.col("Film").is_in(filmy)).select(pl.col("Hrají")).unique()
        s_kolika.append({'Hrají':o,'s_kolika':len(spoluherectvo)})
        print(o)
    return (s_kolika,)


@app.cell
def _(pl, s_kolika):
    pl.DataFrame(s_kolika).sort(by="s_kolika",descending=True).with_row_index(offset=1)
    return


@app.cell
def _(pl, s_kolika):
    pl.DataFrame(s_kolika).sort(by="s_kolika",descending=True).with_row_index(offset=1).filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Doba mezi první a poslední hlavní rolí
    """)
    return


@app.cell
def _(df, pl):
    df.explode("Typologie").filter(pl.col("Typologie") == "hraný").filter(
        pl.col("Minutáž") > 70
    ).with_columns(pl.col("Hrají").list.slice(0,3)).explode("Hrají").group_by("Hrají").agg(
        pl.col("Copyright").min().alias("prvni"),
        pl.col("Copyright").max().alias("posledni")
    ).with_columns(
        (pl.col("posledni") - pl.col("prvni")).alias("rozdil")
    ).sort(by="rozdil",descending=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## S Brzobohatým
    """)
    return


@app.cell
def _(df, pl):
    [x for x in df.explode("Typologie").filter(pl.col("Typologie") == "hraný").filter(pl.col("Minutáž") > 70).with_columns(pl.col("Hrají").list.slice(0,3)).explode("Hrají").filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)").select(pl.col('Film')).to_series().to_list() if x in df.explode("Typologie").filter(pl.col("Typologie") == "hraný").filter(pl.col("Minutáž") > 70).with_columns(pl.col("Hrají").list.slice(0,3)).explode("Hrají").filter(pl.col("Hrají") == "Radoslav Brzobohatý (2770)").select(pl.col('Film')).to_series().to_list()]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Typologie
    """)
    return


@app.cell
def _(df, pl, s_bohdalkou):
    df.filter(
        pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)
    ).explode(["Žánr"]).group_by("Žánr").len().sort(by="len",descending=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Kolegové
    """)
    return


@app.cell
def _(df, pl, s_bohdalkou):
    def zebricek(co, kolik=1000):
        return df.filter(pl.col("Film").is_in(s_bohdalkou) & (pl.col("Minutáž") > 70)).with_columns(pl.col(co).list.slice(0,kolik)).explode(co).group_by(co).len().sort(by="len",descending=True)

    return (zebricek,)


@app.cell
def _(zebricek):
    zebricek("Režie")
    return


@app.cell
def _(zebricek):
    zebricek("Scénář")
    return


@app.cell
def _(zebricek):
    zebricek("Scénář",kolik=1)
    return


@app.cell
def _(zebricek):
    zebricek("Kamera")
    return


@app.cell
def _(zebricek):
    zebricek("Hudba")
    return


@app.cell
def _(zebricek):
    zebricek("Písně")
    return


@app.cell
def _(zebricek):
    zebricek("Kostýmy")
    return


if __name__ == "__main__":
    app.run()
