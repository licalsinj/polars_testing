"""Examples of how to use different data types"""

import polars as pl
import random
from datetime import date

def series():
    # a series is a 1D homogenius data structure 
    # like a list or array it has only rows in a single column

    # create a named series
    s = pl.Series("ints", [1,2,3,4,5])
    print(s)

    # you can overwrite the inference mechanism by seeting dtype=
    s2 = pl.Series("uints", [1, 2, 3, 4, 5], dtype=pl.UInt64)
    print("Data types: ", s.dtype, s2.dtype)

def data_frames() -> pl.DataFrame:
    # data frames are 2 dimensional heterogeneous data structures
    # like a table it has an x and y direction (rows and columns)
    df = pl.DataFrame(
        {
            "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
            "birthdate": [
                date(1997, 1, 10),
                date(1985, 2, 15),
                date(1983, 3, 22),
                date(1981, 4, 30),
            ],
            "weight": [57.9, 72.5, 53.6, 83.1],  # (kg)
            "height": [1.56, 1.77, 1.65, 1.75],  # (m)
        }
    )

    print(df)
    return df
    
def df_inspection(df: pl.DataFrame):
    """Examples of ways to inspect the data frame"""
    # head shows the first rows of the df. 
    # By default you get five. In this case we get 3
    print(df.head(3))

    # glimpse shows the first few rows but formats it differently
    # It switches the columns with rows and prints a list
    print(df.glimpse(return_as_string=True))

    # tail shows the last few rows. 
    # You get 5 by default but can specify less/more
    print(df.tail(2))

    # sample will get an arbitrary number of randomly selected rows from the DF
    random.seed(42)
    print(df.sample(2))

    # describe will compute summary statistics for the DF
    print(df.describe())

def schema_manip(df: pl.DataFrame):
    """Examples of how to use schema information"""
    print(df.schema)

    # you can overwrite the the shema (None will not overwrite it)
    df = pl.DataFrame(
        {
            "name": ["Alice", "Ben", "Chloe", "Daniel"],
            "age": [27, 39, 41, 43],
        },
        schema={"name": None, "age": pl.UInt128}
    )
    print(df.schema)
    print(df)

    # less verbose way to override just one column
    df = pl.DataFrame(
        {
            "name": ["Alice", "Ben", "Chloe", "Daniel"],
            "age": [27, 39, 41, 43],
        },
        schema_overrides={"age": pl.UInt8}
    )
    print(df.schema)
    print(df)

if __name__ == "__main__": 
    # series()
    df = data_frames()
    # df_inspection(df)
    schema_manip(df)
