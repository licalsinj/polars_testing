"""Examples of how to combine dataframes with polars"""
import polars as pl
import datetime as dt

def basic_left_join(df1: pl.DataFrame, df2: pl.DataFrame):
    # left join two dataframes on name
    df_join = df1.join(df2, on="name", how="left")
    print(df_join)

def basic_concatenate(df1: pl.DataFrame, df2: pl.DataFrame):
    df_concat = pl.concat([df1, df3], how="vertical")
    print(df_concat)

if __name__ == "__main__":
    df1 = pl.read_csv("data/output/df_to_csv.csv", try_parse_dates=True)
    df2 = pl.DataFrame(
        {
            "name": ["Bob Brown",  "Daniel Donovan", "Alice Archer", "Chloey Cooper"],
            "parent": [True, False, False, False],
            "siblings": [1, 2, 3, 4],
        }
    )

    df3 = pl.DataFrame(
        {
            "name": ["Ethan Edwards", "Fiona Foster", "Grace Gibson", "Henry Harris"],
            "birthdate": [
                dt.date(1977, 5, 10),
                dt.date(1975, 6, 23),
                dt.date(1973, 7, 22),
                dt.date(1971, 8, 3),
            ],
            "weight": [67.9, 72.5, 57.6, 93.1],  # (kg)
            "height": [1.76, 1.6, 1.66, 1.8],  # (m)
        }
    )

    basic_left_join(df1, df2)
    basic_concatenate(df1, df3)