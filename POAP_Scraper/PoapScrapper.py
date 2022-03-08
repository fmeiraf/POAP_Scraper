import pandas as pd
import requests
import yaml
import os
import json


class PoapScrapper:
    def __init__(
        self,
        poap_event_api_url: str,
        gnosischain_graph_url: str,
        ethereum_graph_url: str,
    ):
        self.event_api_url = poap_event_api_url
        self.gchain_graph_url = gnosischain_graph_url
        self.eth_graph_url = ethereum_graph_url

    def parse(self):
        self.get_event_data()

    def get_event_data(self):
        print("Getting event data.. \n")

        req = requests.get(self.event_api_url)
        if req.status_code != 200:
            raise ValueError("There was a problem with your request to POAP API. \n")

        j = json.loads(req.text)
        self.export_to_json_file(
            content_to_export=j,
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
        if os.path.exists(final_path):
            print("This file already exists ;).")
        else:
            with open(final_path, "w") as outfile:
                json.dump(content_to_export, outfile)


def main():

    # getting links from parameters.yaml
    parameters_file = os.path.join(os.getcwd(), "./POAP_Scraper/parameters.yaml")
    if not os.path.exists(parameters_file):
        raise OSError(
            "You should have a yaml file on your root directory called parameters.yaml! Check the README for more info"
        )

    with open(parameters_file) as file:
        parameters = yaml.load(file, Loader=yaml.FullLoader)

    # initializing and executing scrapper
    scrapper = PoapScrapper(
        poap_event_api_url=parameters["poap_api"],
        gnosischain_graph_url=parameters["gchain_subgraph"],
        ethereum_graph_url=parameters["eth_subgraph"],
    )

    scrapper.parse()


if __name__ == "__main__":
    main()
