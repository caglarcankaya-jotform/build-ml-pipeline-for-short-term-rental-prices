#!/usr/bin/env python
"""
Perform basic cleaning opf parameter given
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def remove_outliers(raw_df, min_price, max_price):
    '''
    Removes outlier datum from given DataFrame based on price 

            Parameters:
                    raw_df (DataFrame): Data
                    min_price (float): Minimum price to keep in data
                    max_price (float): Maximum price to keep in data

            Returns:
                    filtered_df (DataFrame): Filtered dataset according 
                            to maximum and minimum prices
    '''
    logger.info("Removing outliers")
    idx = raw_df['price'].between(min_price, max_price)
    filtered_df = raw_df[idx].copy()
    idx = filtered_df['longitude'].between(-74.25, -73.50) & filtered_df['latitude'].between(40.5, 41.2)
    filtered_df = filtered_df[idx].copy()
    return filtered_df


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Starting the download of the raw artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    raw_df = pd.read_csv(artifact_local_path)
    #Remove outliers
    df = remove_outliers(raw_df=raw_df, max_price= args.max_price, min_price=args.min_price)
    #Set last_review column to datetime format
    logger.info("Converting last_review column data type to datetime")
    df["last_review"] = pd.to_datetime(df["last_review"])

    #Store output file as csv and send it to W&B
    logger.info("Saving to output artifact")
    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A process for cleaning the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input artifact that will be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the input artifact that is cleaned",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description about the output artifact",
        required=True
    )
    
    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price parameter that should be filter data on",
        required=True
    )


    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price parameter that should be filter data on",
        required=True
    )

    args = parser.parse_args()

    go(args)
