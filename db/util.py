import logging
import pandas as pd
from environ import ENV

logger = logging.getLogger(__name__)


def write_df_to_csv(df: pd.DataFrame, index_name: str, table_name: str):
    """Write a dataframe out to a csv (Currently). This will be changed
    
    :param df: Dataframe to write
    :type df: pd.DataFrame
    :param table_name: Table name to use in the csv
    :type table_name: str
    """

    # Drop columns if configured to drop
    try:
        drop_cols = ENV["DROP_DATABASE_COLUMNS"][table_name]
        df = df.drop(drop_cols, axis=1)
    except KeyError:
        logger.info(f"No columns configured to drop for {table_name}, skipping this step.")
    if index_name:
        # Add a named index
        df.index.name = index_name
    # Sort columns alphabetically
    df = df.sort_index(axis=1)

    # Change this to_sql and handle the SQL soon
    df.to_csv(f"{table_name}.csv")
