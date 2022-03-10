import pandas as pd
import requests
import yaml
import os
import json
import utils
import argparse


class PoapScrapper:
    def __init__(
        self,
        poap_event_api_url: str,
        gnosischain_graph_url: str,
        ethereum_graph_url: str,
        outpath: str,
    ):
        self.event_api_url = poap_event_api_url
        self.gchain_graph_url = gnosischain_graph_url
        self.eth_graph_url = ethereum_graph_url
        self.outpath = outpath

    def parse(self):
        self.get_event_data()
        # self.get_token_data()

    def get_token_data(self, page_size: int = 900):

        print("Getting ethereum POAP mainnet subgraph data.")
        eth_token_data = utils.extract_all_tokens_from_subgraph(
            subgraph_api_url=self.eth_graph_url,
            page_size=page_size,
        )

        self.export_to_json_file(
            content_to_export=eth_token_data,
            filename_without_extension="poap_ethereum_token_data",
        )

        print("Getting POAP gnosis chain subgraph data.")
        gchain_token_data = utils.extract_all_tokens_from_subgraph(
            subgraph_api_url=self.gchain_graph_url,
            page_size=page_size,
        )

        self.export_to_json_file(
            content_to_export=gchain_token_data,
            filename_without_extension="poap_xdai_token_data",
        )

    def get_event_data(self):
        print("Getting event data.. \n")

        req = requests.get(self.event_api_url)
        if req.status_code != 200:
            raise ValueError("There was a problem with your request to POAP API. \n")

        j = json.loads(req.text)
        self.export_to_json_file(
            content_to_export=j,
            dir_to_export_to=self.outpath,
            filename_without_extension="poap_event_data",
        )

        print("Finished gathering POAP event data. \n")

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
    # parser.add_argument(
    #     "-s",
    #     "--space",
    #     metavar="space_id",
    #     type=str,
    #     help="IF restricting to one space (DAO), the id of that DAO. For example: yam.eth",
    # )

    args = parser.parse_args()

    # initializing and executing scrapper
    scrapper = PoapScrapper(
        poap_event_api_url=parameters["poap_api"],
        gnosischain_graph_url=parameters["gchain_subgraph"],
        ethereum_graph_url=parameters["eth_subgraph"],
        outpath=args.out,
    )

    scrapper.parse()


if __name__ == "__main__":
    main()
