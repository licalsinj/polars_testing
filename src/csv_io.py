"""Some examples of input/output with csv files"""

import polars as pl
import datetime as dt

def df_to_csv():
    # manually create a dataframe
    df = pl.DataFrame(
        {
            "name": ["Alice Archer", "Bob Brown", "Chloey Cooper", "Daniel Donovan"],
            "birthdate": [
                dt.date(1992, 1, 10),
                dt.date(1992, 2, 15),
                dt.date(1984, 5, 25),
                dt.date(1999, 7, 4),                
            ],
            "weight": [57.9, 72.5, 53.6, 83.1], # kilograms
            "height": [1.56, 1.77, 1.65, 1.75], # meters
        }
    )
    # print the df to the console
    print(df)
    # save it to a csv
    df.write_csv("data/output/df_to_csv.csv")
    # read it back out of the csv
    df_csv = pl.read_csv("data/output/df_to_csv.csv", try_parse_dates=True)
    # print it to see how it goes 
    print(df_csv)



if __name__ == "__main__":
    df_to_csv()