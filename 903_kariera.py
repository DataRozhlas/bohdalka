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
    pl.read_parquet("data/filmy.parquet").filter(
        pl.col("format").is_in(["(epizoda)", "(TV film)"])
    )
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

    animaky_csfd = (
        pl.read_parquet("data/filmy.parquet")
        .explode("zanry")
        .filter(pl.col("zanry") == "animovaný")
        .select(pl.col("nazev"))
        .to_series()
        .to_list()
    )

    doplneni1 = (
        (
            pl.read_parquet("data/filmy.parquet")
            .filter(pl.col("format").is_in(["(epizoda)", "(TV film)"]))
            .filter(~pl.col("nazev").is_in(dokumentarni_csfd))
            .filter(~pl.col("nazev").is_in(animaky_csfd))
            .with_columns(
                pl.lit(["x", "Jiřina Bohdalová (2878)"]).alias("Hrají"),
                pl.lit(["hraný"]).alias("Typologie"),
            )
        )
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
        .with_columns(
            (
                1
                + pl.col("hrají")
                .list.eval(
                    pl.arg_where(pl.element() == pl.lit("Jiřina Bohdalová"))
                )
                .list.first()
            ).alias("kolikata_bohdalka")
        )
        .with_columns(
            pl.when(pl.col("plot").str.contains(("(?i)pohád")))
            .then(pl.lit(["pohádka"]))
            .otherwise(pl.col("zanry"))
            .alias("zanry")
        )
    )

    sloupce_csfd = doplneni1.columns

    doplneni2 = (
        pl.read_parquet("data/filmy.parquet")
        .filter(~pl.col("nazev").is_in(dokumentarni_csfd))
        .filter(~pl.col("nazev").is_in(animaky_csfd))
        .filter(pl.col("rok") >= 2017)
        .with_columns(
            pl.lit(["x", "Jiřina Bohdalová (2878)"]).alias("Hrají"),
            pl.lit(["hraný"]).alias("Typologie"),
        )
        .rename({"rok": "Copyright", "nazev": "Film", "stopaz": "Minutáž"})
        .with_columns(
            (
                1
                + pl.col("hrají")
                .list.eval(
                    pl.arg_where(pl.element() == pl.lit("Jiřina Bohdalová"))
                )
                .list.first()
            ).alias("kolikata_bohdalka")
        )
    )

    s_bohdalkou = (
        pl.read_parquet("../filmovy-prehled/data/filmy.parquet")
        .explode("Hrají")
        .filter(pl.col("Hrají") == "Jiřina Bohdalová (2878)")
        .explode("Typologie")
        .filter(pl.col("Typologie") == "hraný")
        .select(pl.col("Film"))
        .to_series()
        .to_list()
    )

    df = pl.concat(
        [
            pl.read_parquet("../filmovy-prehled/data/filmy.parquet")
            .filter(pl.col("Film").is_in(s_bohdalkou))
            .with_columns(pl.col("Film").str.split(" (").list.slice(0, 1))
            .explode("Film")
            .sort(by="Copyright", descending=True),
            doplneni1,
            doplneni2,
        ],
        how="diagonal_relaxed",
    )
    return df, doplneni1, sloupce_csfd


@app.cell
def _(doplneni1, pl):
    doplneni1.filter(pl.col("Film").str.contains("Dals"))
    return


@app.cell
def _(pl):
    role_z_filmoveho_prehledu = pl.read_json("data/role_z_filmoveho_prehledu.json")
    return (role_z_filmoveho_prehledu,)


@app.cell
def _(df):
    df.sort(by="Film")
    return


@app.cell
def _(df, pl, role_z_filmoveho_prehledu):
    filmove_role = (
        (
            df.filter(
                pl.col("Země původu").is_not_null()
                | (pl.col("Film") == "Vánoční příběh")
            )
            .with_columns(
                (
                    1
                    + pl.col("Hrají")
                    .list.eval(
                        pl.arg_where(
                            pl.element() == pl.lit("Jiřina Bohdalová (2878)")
                        )
                    )
                    .list.first()
                ).alias("kolikata_bohdalka")
            )
            .select(pl.col(["Copyright", "Film", "kolikata_bohdalka", "Žánr"]))
            .sort(by="Copyright")
            .with_columns(pl.lit("film").alias("typ"))
        )
        .with_columns(
            pl.when(pl.col("kolikata_bohdalka") <= 3)
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("hlavni"),
            pl.col("Žánr").list.join(", "),
        )
        .rename({"Copyright": "rok", "Film": "film", "Žánr": "žánry"})
        .drop("kolikata_bohdalka")
        .join(role_z_filmoveho_prehledu, how="left", on=["film", "rok"])
        .with_columns(
            pl.when(pl.col("film") == "Glorie")
            .then(pl.lit("dívka v průjezdu"))
            .when(pl.col("film") == "Tajemství písma")
            .then(pl.lit("prodavačka ve starožitnictví"))
            .when(pl.col("film") == "Vánoční příběh")
            .then(pl.lit("herečka Skálová"))
            .otherwise(pl.col("role"))
            .alias("role")
        )
    )

    filmove_role
    return (filmove_role,)


@app.cell
def _():
    return


@app.cell
def _(df, pl, sloupce_csfd):
    tv_role = (
        (
            df.filter(pl.col("Země původu").is_null())
            .select(pl.col(sloupce_csfd))
            .rename({"Film": "film", "Copyright": "rok"})
            .with_columns(pl.lit(None).alias("hlavni"), pl.lit(None).alias("role"))
            .with_columns(
                pl.when(pl.col("epizoda").is_not_null())
                .then(pl.col("film") + pl.lit(": ") + pl.col("epizoda"))
                .otherwise(pl.col("film"))
                .alias("film"),
                pl.when(pl.col("kolikata_bohdalka") <= 3)
                .then(pl.lit(True))
                .otherwise(pl.lit(False))
                .alias("hlavni"),
            )
            .with_columns(
                pl.when(pl.col("epizoda").is_not_null())
                .then(pl.lit("seriál"))
                .otherwise(pl.lit("TV film"))
                .alias("typ")
            )
            .select(pl.col(["rok", "film", "zanry", "typ", "hlavni", "role"]))
        )
        .with_columns(pl.col("zanry").list.join(", "))
        .rename({"zanry": "žánry"})
    )

    tv_role
    return (tv_role,)


@app.cell
def _(filmove_role, pl, tv_role):
    role = (
        pl.concat([filmove_role, tv_role])
        .sort(by="rok")
        .unique(subset="film")
        .with_columns(
            pl.when(pl.col("žánry").str.contains_any(["dětský", "pohádka"]))
            .then(pl.lit("pohádka"))
            .when(pl.col("žánry").str.contains("komedie"))
            .then(pl.lit("komedie"))
            .when(pl.col("žánry").str.contains("drama"))
            .then(pl.lit("drama"))
            .alias("žánr"),
            pl.col("hlavni").fill_null(pl.lit(False)),
            pl.col("film").str.replace("\n", " ").str.replace(r"\s{2,1000}", " "),
        )
        .with_columns(
            pl.when(
                pl.col("film").str.contains_any(
                    ["Racoch", "lišce", "ovčí", "pohádek", "Rákosn", "Pohád"]
                )
            )
            .then(pl.lit("pohádka"))
            .otherwise(pl.col("žánr"))
            .alias("žánr")
        )
        .drop("žánry")
    )
    return (role,)


@app.cell
def _(pl, role):
    role.filter(pl.col("film").str.contains("Svatá"))
    return


@app.cell
def _(role):
    role.write_json("data/role.json")
    return


if __name__ == "__main__":
    app.run()
