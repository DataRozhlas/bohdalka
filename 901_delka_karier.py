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
def _(cs, pl):
    dokumentarni = (
        pl.read_parquet("data/filmy.parquet")
        .explode("zanry")
        .filter(pl.col("zanry") == "dokumentární")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )

    print(dokumentarni[0:5])

    df_doplneni = (
        pl.read_parquet("data/filmy.parquet")
        .filter(pl.col("format").is_null())
        .filter(~pl.col("nazev").is_in(dokumentarni))
        .with_columns(pl.lit(["Jiřina Bohdalová (2878)"]).alias("Hrají"))
        .filter(pl.col("rok") > 2016)
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
    )

    df = (
        pl.concat(
            [
                df_doplneni,
                pl.read_parquet("../filmovy-prehled/data/filmy.parquet"),
            ],
            how="diagonal_relaxed",
        )
        .filter(~pl.col("Film").is_in(dokumentarni))
        .drop(
            [
                "Hudba",
                "Použitá hudba",
                "Text písně",
                "Zpívá",
                "Hudba, text a zpěv písně",
                "Výběr hudby",
            ]
        )
        .with_columns(cs.by_dtype(pl.List).fill_null([]))
        .with_columns(pl.concat_list(cs.by_dtype(pl.List)).alias("lide"))
        .sort(by="Copyright", descending=True)
    )

    persony = pl.read_parquet("../filmovy-prehled/data/persony.parquet")
    return df, persony


@app.cell
def _(df):
    ", ".join(df.columns)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Kariéra by byla ještě o 2 roky delší, kdyby se počítal film TV film Svatá.
    """)
    return


@app.cell
def _(df, pl):
    dokumentarni2 = (
        df.explode("Typologie")
        .filter(pl.col("Typologie") == "dokumentární")
        .select(pl.col("Film"))
        .to_series()
        .to_list()
    )
    return (dokumentarni2,)


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)").sort(
        by="Copyright"
    )
    return


@app.cell
def _(df):
    df
    return


@app.cell
def _(df, dokumentarni2, persony, pl):
    df.explode("Hrají").select("Film", "Copyright", "Hrají", "Minutáž").join(
        persony.select("Jméno", "Rok narození", "Rok úmrtí"),
        left_on="Hrají",
        right_on="Jméno",
        how="left",
    ).filter(
        (pl.col("Rok úmrtí").is_null())
        | (pl.col("Copyright") < pl.col("Rok úmrtí"))
    ).filter(~pl.col("Film").is_in(dokumentarni2)).group_by("Hrají").agg(
        pl.col("Rok narození").first(),
        pl.col("Rok úmrtí").first(),
        pl.col("Copyright").min().alias("prvni"),
        pl.col("Copyright").max().alias("posledni"),
    ).with_columns(
        (pl.col("posledni") - pl.col("prvni")).alias("delka_kariery")
    ).filter(pl.col("Hrají").str.contains("\(")).sort(
        by="delka_kariery", descending=True
    )
    return


@app.cell
def _(df, dokumentarni2, persony, pl):
    for i in range(2000, 2020):
        print(i)
        print(
            df.filter(pl.col("Copyright") <= i)
            .explode("Hrají")
            .select("Film", "Copyright", "Hrají", "Minutáž")
            .join(
                persony.select("Jméno", "Rok narození", "Rok úmrtí"),
                left_on="Hrají",
                right_on="Jméno",
                how="left",
            )
            .filter(
                (pl.col("Rok úmrtí").is_null())
                | (pl.col("Copyright") < pl.col("Rok úmrtí"))
            )
            .filter(~pl.col("Film").is_in(dokumentarni2))
            .group_by("Hrají")
            .agg(
                pl.col("Rok narození").first(),
                pl.col("Rok úmrtí").first(),
                pl.col("Copyright").min().alias("prvni"),
                pl.col("Copyright").max().alias("posledni"),
            )
            .with_columns(
                (pl.col("posledni") - pl.col("prvni")).alias("delka_kariery")
            )
            .filter(pl.col("Hrají").str.contains("\("))
            .sort(by="delka_kariery", descending=True)
            .head(3)
        )
    return


@app.cell
def _(df, dokumentarni2, persony, pl):
    df.explode("lide").select("Film", "Copyright", "lide", "Minutáž").join(
        persony.select("Jméno", "Rok narození", "Rok úmrtí"),
        left_on="lide",
        right_on="Jméno",
        how="left",
    ).filter(
        (
            (pl.col("Rok úmrtí").is_null())
            | (pl.col("Copyright") < pl.col("Rok úmrtí"))
        )
        & (pl.col("Rok narození").is_not_null())
    ).filter(~pl.col("Film").is_in(dokumentarni2)).group_by("lide").agg(
        pl.col("Rok narození").first(),
        pl.col("Rok úmrtí").first(),
        pl.col("Copyright").min().alias("prvni"),
        pl.col("Copyright").max().alias("posledni"),
    ).with_columns(
        (pl.col("posledni") - pl.col("prvni")).alias("delka_kariery")
    ).sort(by="delka_kariery", descending=True)
    return


@app.cell
def _(persony, pl):
    persony.filter(pl.col("Jméno").str.contains("Svatopluk Beneš"))
    return


@app.cell
def _():
    nestorstvo = [
        x.split("(")[0].strip()
        for x in [
            "Jiřina Bohdalová (2878)",
            "Zdenka Procházková (42989)",
            "Zita Kabátová (839)",
            "Jiří Novotný (9283)",
            "Svatopluk Beneš (13617)",
            "Lubomír Lipský (9728)",
            "Vladimír Hlavatý (34999)",
            "Břetislav Pojar (3113)",
            "Vladimír Brabec (3858)",
            "Saskia Burešová (25639)",
        ]
    ]
    print(len(nestorstvo))
    print(nestorstvo)
    return (nestorstvo,)


@app.cell
def _(df, nestorstvo, pl):
    df.explode("Hrají").filter(pl.col("Hrají").is_in(nestorstvo)).group_by(
        ["Hrají", "Copyright"]
    ).len()
    return


@app.cell
def _(df, dokumentarni2, nestorstvo, pl):
    do_grafu = (
        df.filter(~pl.col("Film").str.contains("Pižla"))
        .explode("lide")
        .with_columns(pl.col("lide").str.split("(").list.slice(0, 1))
        .explode("lide")
        .with_columns(pl.col("lide").str.strip_chars())
        .filter(pl.col("lide").is_in(nestorstvo))
        .filter(~pl.col("Film").is_in(dokumentarni2))
        .group_by(["lide", "Copyright"])
        .len()
        .rename({"lide": "kdo", "Copyright": "rok"})
    )

    razeni = do_grafu.select(pl.col("kdo")).to_series().to_list()
    return (do_grafu,)


@app.cell
def _(df, dokumentarni2, nestorstvo, pl):
    do_grafu_komplet = (
        (
            df.explode("lide")
            .with_columns(pl.col("lide").str.split("(").list.slice(0, 1))
            .explode("lide")
            .with_columns(pl.col("lide").str.strip_chars())
            .filter(pl.col("lide").is_in(nestorstvo))
            .filter(~pl.col("Film").is_in(dokumentarni2))
            .rename({"lide": "kdo", "Copyright": "rok", "Film": "film"})
            .select(pl.col(["kdo", "rok", "film"]))
            .unique()
        )
        .with_columns(pl.col("film").str.split(" (").list.slice(0, 1))
        .explode("film")
        .sort(by="rok")
    )
    return (do_grafu_komplet,)


@app.cell
def _(do_grafu_komplet):
    do_grafu_komplet
    return


@app.cell
def _():
    import json

    return (json,)


@app.cell
def _(do_grafu_komplet, json):
    json.dumps(do_grafu_komplet.to_dict(as_series=False))
    return


@app.cell
def _(alt, do_grafu, nestorstvo):
    alt.Chart(
        do_grafu,
        width=800,
        height=50,
    ).mark_bar().encode(
        alt.X("rok:Q"),
        alt.Y("len:Q"),
        alt.Row(
            "kdo:N",
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
def _(nestorstvo):
    print(nestorstvo)
    return


@app.cell
def _(do_grafu, pl):
    do_grafu_simple = (
        do_grafu.group_by("kdo")
        .agg(
            pl.col("rok").min().alias("prvni"),
            pl.col("rok").max().alias("posledni"),
        )
        .with_columns(
            pl.col("prvni").cast(str).alias("prvni_text"),
            pl.col("posledni").cast(str).alias("posledni_text"),
        )
        .with_columns((pl.col("posledni") - pl.col("prvni")).alias("trvani"))
        .with_columns(
            (pl.col("trvani").cast(str) + pl.lit(" let")).alias("trvani_text"),
            (pl.col("prvni") + (pl.col("trvani") / 2)).alias("prostredek"),
        )
    )

    do_grafu_simple
    return (do_grafu_simple,)


@app.cell
def _(do_grafu_simple):
    do_grafu_simple.write_json("data/nejdelsi_kariery.json")
    return


@app.cell
def _(do_grafu_simple, json):
    json.dumps(do_grafu_simple.to_dict(as_series=False))
    return


@app.cell
def _(alt, do_grafu, nestorstvo):
    alt.Chart(
        do_grafu,
        width=800,
        height=200,
    ).mark_line().encode(
        alt.X("rok:Q", scale=alt.Scale(domain=[1900, 2030])),
        alt.Y("kdo:N", sort=nestorstvo),
        # This ensures each actor gets their own separate line
        detail="kdo:N",
        # This ensures the line connects years in the correct order
        order="rok:Q",
    )
    return


@app.cell
def _(alt, do_grafu):
    alt.Chart(
        do_grafu,
        width=800,
        height=200,
    ).mark_circle().encode(
        alt.X("rok:Q", scale=alt.Scale(domain=[1900, 2030])),
        alt.Y("kdo:N"),
        alt.Size("len:Q"),
        # This ensures each actor gets their own separate line
        detail="kdo:N",
        # This ensures the line connects years in the correct order
        order="rok:Q",
    )
    return


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Zita Kabátová (839)")
    return


@app.cell
def _(df, pl):
    df.explode("Hrají").filter(pl.col("Hrají") == "Vlasta Burian (11992)").select(
        pl.col("Copyright")
    ).median()
    return


if __name__ == "__main__":
    app.run()
