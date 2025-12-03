import polars as pl

def basic_select(df: pl.DataFrame):
    """Things you can do inside of the select context"""
    df = pl.read_csv("data/output/df_to_csv.csv", try_parse_dates=True)

    # for the given columns slice out their birth year 
    # and calculate their bmi based on existing data in df
    result = df.select(
        pl.col("name"),
        pl.col("birthdate").dt.year().alias("birth_year"),
        (pl.col("weight")/(pl.col("height") ** 2)).alias("bmi"),
    )
    print(result)

    # expression expansion
    # One expression acts as a short hand for multiple
    # here we're manipulating height and weight in the same way
    result = df.select(
        pl.col("name"),
        # manipulate the column & rename it with a suffix
        (pl.col("weight", "height") * .95).round(2).name.suffix("-5%"),
    )
    print(result)


def basic_with_columns(df: pl.DataFrame):
    """Things you can do inside the with_columns context"""
    # with_columns tacks on columns to the existing dataframe
    result = df.with_columns(
        # using named expressions instead of alias 
        birth_year=pl.col("birthdate").dt.year(),
        bmi=pl.col("weight")/(pl.col("height") ** 2),
    )
    print(result)

def basic_filter(df: pl.DataFrame):
    """Things you can do in the filter context"""
    result = df.filter(pl.col("birthdate").dt.year() < 1990)
    print(result)
    # running multiple will && them
    result = df.filter(
        pl.col("birthdate").dt.year() > 1990,
        pl.col("height") > 1.7,
        )
    print(result)

def basic_group_by(df: pl.DataFrame):
    """Examples of what you can do in the group by context"""
    result = df.group_by(
        # determine the decade of each row
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
        # slows down the query but keeps it in the order the rows are found in the source data
        maintain_order=True,
    ).len() # counts the number of each in the group_by
    print(result)

    result = df.group_by(
        (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
        maintain_order=True,
    ).agg(
        # repeat of the len() function above
        pl.len().alias("sample_size"),
        # for all weights in a group mean/average them together
        pl.col("weight").mean().round(2).alias("avg_weight"),
        # for all heights in the group pick the max
        pl.col("height").max().alias("tallest"),
    )
    print(result)

def basic_all_contexts(df: pl.DataFrame):
    result = (
        df.with_columns(
            (pl.col("birthdate").dt.year() // 10 * 10).alias("decade"),
            first_name=pl.col("name").str.split(by=" ").list.first(),
        )
        .select(
            pl.all().exclude("birthdate", "name"),
        )
        .group_by(
            pl.col("decade"),
            maintain_order=True,
        )
        .agg(
            pl.col("first_name"),
            pl.col("weight", "height").mean().round(2).name.prefix("avg_"),
            pl.col("weight", "height").max().round(2).name.prefix("max_"),
            pl.col("weight", "height").min().round(2).name.prefix("min_"),
        )
        .select(
            pl.col("decade"),
            pl.col("first_name"),
            pl.col("avg_weight"),
            pl.col("min_weight"),
            pl.col("max_weight"),
            pl.col("avg_height"),
            pl.col("min_height"),
            pl.col("max_height"),
        )

    )
    print(result)

if __name__ == "__main__":
    df = pl.read_csv("data/output/df_to_csv.csv", try_parse_dates=True)
    print(df)
    basic_select(df)
    basic_with_columns(df)
    basic_filter(df)
    basic_group_by(df)
    basic_all_contexts(df)