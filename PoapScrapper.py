import pandas as pd
import requests
import yaml
import os
import json
import utils
import argparse
import math


class PoapScrapper:
    def __init__(
        self,
        poap_event_api_url: str,
        gnosischain_graph_url: str,
        ethereum_graph_url: str,
        outpath: str,
        use_checkpoints: bool,
    ):
        self.event_api_url = poap_event_api_url
        self.gchain_graph_url = gnosischain_graph_url
        self.eth_graph_url = ethereum_graph_url
        self.outpath = outpath
        self.use_checkpoints = use_checkpoints

    def parse(self):
        print("## Initializing parsing.. ## \n")
        self.get_event_data()
        self.get_token_data()

    def get_token_data(self, page_size: int = 900):

        ## checking for checkpoint use
        if self.use_checkpoints:
            checkpoint_file = os.path.join(
                os.getcwd(), self.outpath, "checkpoints.json"
            )
            if not os.path.exists(checkpoint_file):
                raise OSError(
                    """You should have a json file on your root directory called checkpoints.json! If you don't want to use
                    that, run the script without -c as parameter.
                    """
                )
            with open(checkpoint_file, "r") as readfile:
                checkpoints = json.load(readfile)

            ethereum_last_timestamp = checkpoints["ethereum"]
            gnosis_last_timestamp = checkpoints["gnosis_chain"]
            print(
                f"Using last timestamp checkpoints: ethereum={ethereum_last_timestamp}, gnosis_chain={gnosis_last_timestamp} "
            )
        else:
            ethereum_last_timestamp = 0
            gnosis_last_timestamp = 0
            print(f"Downloading ALL tokens since inception.")

        # getting data from subgraphs
        print("Getting ethereum POAP mainnet subgraph data.")
        eth_token_data = utils.extract_all_tokens_from_subgraph(
            subgraph_api_url=self.eth_graph_url,
            page_size=page_size,
            last_timestamp=ethereum_last_timestamp,
        )

        print("Getting POAP gnosis chain subgraph data.")
        gchain_token_data = utils.extract_all_tokens_from_subgraph(
            subgraph_api_url=self.gchain_graph_url,
            page_size=page_size,
            last_timestamp=gnosis_last_timestamp,
        )

        # consolidating data and saving the results
        print("Consolidating info and saving..")
        ethtokens = pd.read_json(json.dumps(eth_token_data))
        ethtokens["chain"] = "ethereum"

        gchaintokens = pd.read_json(json.dumps(gchain_token_data))
        gchaintokens["chain"] = "gnosis_chain"

        # checking for empty datasets
        datasets_to_combine = []
        for dataset in [ethtokens, gchaintokens]:
            if len(dataset) != 0:
                datasets_to_combine.append(dataset)

        token_data = pd.concat([ethtokens, gchaintokens], sort=True)

        final_path = os.path.join(os.getcwd(), self.outpath, "token_data.json")
        token_data.to_json(final_path, orient="records")

        # saving checkpoints
        print("Saving checkpoints..")
        checkpoint_obj = {}

        # checking for NaN values before assigning to the checkpoint file
        max_eth_timestamp = token_data.loc[
            token_data.chain == "ethereum", "token_created"
        ].max()
        max_gnosis_timestamp = token_data.loc[
            token_data.chain == "gnosis_chain", "token_created"
        ].max()

        # pdb.set_trace()

        checkpoint_obj["ethereum"] = (
            int(max_eth_timestamp)
            if not math.isnan(max_eth_timestamp)
            else ethereum_last_timestamp
        )
        checkpoint_obj["gnosis_chain"] = (
            int(max_gnosis_timestamp)
            if not math.isnan(max_gnosis_timestamp)
            else gnosis_last_timestamp
        )

        checkpoint_path = final_path = os.path.join(
            os.getcwd(), self.outpath, "checkpoints.json"
        )
        with open(checkpoint_path, "w") as outfile:
            json.dump(checkpoint_obj, outfile)

        print("Checkpoints are SAVED.")
        print("Token data gathering is DONE.")

    def get_event_data(self):
        print("Getting event data..")

        req = requests.get(self.event_api_url)
        if req.status_code != 200:
            raise ValueError("There was a problem with your request to POAP API. \n")

        j = json.loads(req.text)
        self.export_to_json_file(
            content_to_export=j,
            dir_to_export_to=self.outpath,
            filename_without_extension="poap_event_data",
        )

        print("Event data gathering is DONE. \n")

    def export_to_json_file(
        self,
        content_to_export: object,
        filename_without_extension: str,
        dir_to_export_to: str = "POAP_Scraper/",
    ):

        final_path = os.path.join(
            os.getcwd(), dir_to_export_to, f"{filename_without_extension}.json"
        )

        with open(final_path, "w") as outfile:
            json.dump(content_to_export, outfile)


def main():

    # getting links from parameters.yaml
    parameters_file = os.path.join(os.getcwd(), "parameters.yaml")
    if not os.path.exists(parameters_file):
        raise OSError(
            "You should have a yaml file on your root directory called parameters.yaml! Check the README for more info"
        )

    with open(parameters_file) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)

    parser = argparse.ArgumentParser(description="POAP Scrapper.")
    parser.add_argument(
        "-o",
        "--out",
        metavar="out_path",
        type=str,
        required=True,
        help="The location for the folder containing the result of the scrapping.",
    )
    parser.add_argument(
        "-c",
        "--checkpoints",
        action="store_true",
        help="This will enable the script to assess the last scrapped tokens using the checkpoints.json file",
    )

    args = parser.parse_args()

    # initializing and executing scrapper
    scrapper = PoapScrapper(
        poap_event_api_url=parameters["poap_api"],
        gnosischain_graph_url=parameters["gchain_subgraph"],
        ethereum_graph_url=parameters["eth_subgraph"],
        outpath=args.out,
        use_checkpoints=args.checkpoints,
    )

    scrapper.parse()


if __name__ == "__main__":
    main()
