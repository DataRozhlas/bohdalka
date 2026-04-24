import marimo

__generated_with = "0.23.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    import polars.selectors as cs
    import altair as alt

    return alt, cs, mo, pl


@app.cell
def _():
    return


@app.cell
def _(cs, pl):
    dokumentarni = (
        pl.read_parquet("data/filmy.parquet")
        .explode("zanry")
        .filter(pl.col("zanry") == "dokumentární")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )

    df_doplneni = (
        pl.read_parquet("data/filmy.parquet")
        .filter(pl.col("format").is_null())
        .filter(~pl.col("nazev").is_in(dokumentarni))
        .with_columns(pl.lit(["Jiřina Bohdalová (2878)"]).alias("Hrají"))
        .filter(pl.col("rok") > 2016)
        .rename({"rok": "Copyright", "nazev": "Film","stopaz":"Minutáž"})
    )

    df = pl.concat(
        [df_doplneni, pl.read_parquet("../filmovy-prehled/data/filmy.parquet")],
        how="diagonal_relaxed",
    ).with_columns(
        cs.by_dtype(pl.List).fill_null([])
    ).with_columns(
        pl.concat_list(cs.by_dtype(pl.List)).alias("lide")
    ).sort(by='Copyright',descending=True)

    persony = pl.read_parquet("../filmovy-prehled/data/persony.parquet")
    return df, persony


@app.cell
def _():
    return


@app.cell
def _(df, pl):
    df.select(pl.col("lide"))
    return


@app.cell
def _(persony):
    persony
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Kariéra by byla ještě o 2 roky delší, kdyby se počítal film TV film Svatá.
    """)
    return


@app.cell
def _(df, pl):
    dokumentarni2 = df.explode("Typologie").filter(pl.col("Typologie") == "dokumentární").select(pl.col("Film")).to_series().to_list()
    return (dokumentarni2,)


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)").sort(by="Copyright")
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _(df, dokumentarni2, persony, pl):
    df.explode('Hrají').select('Film','Copyright','Hrají',"Minutáž").join(
        persony.select("Jméno","Rok narození","Rok úmrtí"),
        left_on="Hrají",
        right_on="Jméno",
        how="left"
    ).filter(
        (pl.col("Rok úmrtí").is_null()) | (pl.col("Copyright") < pl.col("Rok úmrtí"))
    ).filter(~pl.col("Film").is_in(dokumentarni2)).group_by(
        'Hrají'
    ).agg(
        pl.col("Rok narození").first(),
        pl.col("Rok úmrtí").first(),
        pl.col("Copyright").min().alias("prvni"),
        pl.col("Copyright").max().alias("posledni")
    ).with_columns(
        (pl.col("posledni") - pl.col("prvni")).alias("delka_kariery")
    ).sort(by="delka_kariery",descending=True)
    return


@app.cell
def _(df, dokumentarni2, persony, pl):
    df.explode('lide').select('Film','Copyright','lide',"Minutáž").join(
        persony.select("Jméno","Rok narození","Rok úmrtí"),
        left_on="lide",
        right_on="Jméno",
        how="left"
    ).filter(
        ((pl.col("Rok úmrtí").is_null()) | (pl.col("Copyright") < pl.col("Rok úmrtí"))) & (pl.col('Rok narození').is_not_null())
    ).filter(~pl.col("Film").is_in(dokumentarni2)).group_by(
        'lide'
    ).agg(
        pl.col("Rok narození").first(),
        pl.col("Rok úmrtí").first(),
        pl.col("Copyright").min().alias("prvni"),
        pl.col("Copyright").max().alias("posledni")
    ).with_columns(
        (pl.col("posledni") - pl.col("prvni")).alias("delka_kariery")
    ).sort(by="delka_kariery",descending=True)
    return


@app.cell
def _():
    nestorstvo = [
        'Jiřina Bohdalová (2878)',
        'Jiří Suchý (2270)',
        'Zdenka Procházková (42989)',
        'Zita Kabátová (839)',
        'Soňa Červená (10930)',
        'Svatopluk Beneš (13617)',
        'František Kovářík (126912)',
        'Jiří Novotný (9283)',
        'Břetislav Pojar (3113)',
        'Vladimír Brabec (3858)',
        'Lubomír Lipský (9728)'
    ]
    len(nestorstvo)
    return (nestorstvo,)


@app.cell
def _(df, nestorstvo, pl):
    df.explode('Hrají').filter(pl.col("Hrají").is_in(nestorstvo)).group_by(["Hrají","Copyright"]).len()
    return


@app.cell
def _(df, dokumentarni2, nestorstvo, pl):
    do_grafu = (
        df.explode("lide")
        .filter(pl.col("lide").is_in(nestorstvo))
        .filter(~pl.col("Film").is_in(dokumentarni2))
        .group_by(["lide", "Copyright"])
        .len()
        .with_columns(pl.col("lide").str.split("(").list.slice(0,1)).explode("lide")
    )

    razeni = do_grafu.select(pl.col('lide')).to_series().to_list()
    return (do_grafu,)


@app.cell
def _(alt, do_grafu, nestorstvo):
    alt.Chart(
        do_grafu,
        width=800,
        height=50,
    ).mark_bar().encode(
        alt.X("Copyright:Q"),
        alt.Y("len:Q"),
        alt.Row(
            "lide:N",
            sort=nestorstvo,
            header=alt.Header(
                labelAngle=0,
                labelAlign="left",
                labelAnchor="start",
                labelFontWeight=500,
                labelFont="Asap",
                labelOrient="top",
            ),
        ),
    )
    return


@app.cell
def _(do_grafu):
    do_grafu
    return


@app.cell
def _(nestorstvo):
    nestorstvo
    return


@app.cell
def _(alt, do_grafu, nestorstvo):
    alt.Chart(
        do_grafu,
        width=800,
        height=200,
    ).mark_line().encode(
        alt.X("Copyright:Q", scale=alt.Scale(domain=[1900, 2030])),
        alt.Y("lide:N", sort=nestorstvo),
        # This ensures each actor gets their own separate line
        detail="lide:N",
        # This ensures the line connects years in the correct order
        order="Copyright:Q"
    )
    return


@app.cell
def _(alt, do_grafu):
    alt.Chart(
        do_grafu,
        width=800,
        height=200,
    ).mark_circle().encode(
        alt.X("Copyright:Q", scale=alt.Scale(domain=[1900, 2030])),
        alt.Y("Hrají:N"),
        alt.Size("len:Q"),
        # This ensures each actor gets their own separate line
        detail="Hrají:N",
        # This ensures the line connects years in the correct order
        order="Copyright:Q"
    )
    return


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == 'Zita Kabátová (839)')
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Vlasta Burian (11992)").select(pl.col("Copyright")).median()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
