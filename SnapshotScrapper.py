#!/usr/bin/env python

import argparse
import json
import os
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import time
import urllib3

urllib3.disable_warnings()
import tqdm as tqdm


class SnapshotScapper:
    def __init__(self, base_url, headers=None, outpath="."):
        self.base_url = base_url
        if not headers:
            self.headers = {
                "Content-type": "application/json",
            }
        else:
            self.headers = headers

        self.sample_transport = RequestsHTTPTransport(
            url=self.base_url,
            use_json=True,
            headers=self.headers,
            verify=False,
            retries=3,
        )

        self.client = Client(
            transport=self.sample_transport,
            fetch_schema_from_transport=True,
        )

        self.outpath = outpath
        self.results_path = os.path.join(outpath, "results")
        self.spaces_path = os.path.join(self.results_path, "spaces")

        os.makedirs(self.spaces_path, exist_ok=True)

    def parse(self, spaces=[]):
        if not spaces:
            spaces = self.get_spaces(data=[])

        with open(os.path.join(self.results_path, "spaces.json"), "w") as f:
            f.write(json.dumps(spaces))

        for space in tqdm.tqdm(spaces, position=0):
            self.parse_space(space["id"])

    def parse_space(self, space_id):
        result = {"id": space_id, "proposals": []}
        space_path = os.path.join(self.spaces_path, space_id)
        try:
            os.mkdir(os.path.join(self.spaces_path, space_id))
        except:
            pass
        proposals = self.get_proposals([space_id], data=[])
        for proposal in tqdm.tqdm(proposals, position=1):
            if proposal["votes"] > 0:
                votes = self.get_votes(proposal["id"], data=[])
                if not votes:
                    print("Error on proposal: ", proposal["id"])
            else:
                votes = []

            proposal["votes_data"] = votes
            with open(os.path.join(space_path, proposal["id"] + ".json"), "w") as f:
                f.write(json.dumps(proposal))
            result["proposals"].append(proposal)
        return result

    def get_votes(self, proposal_id, data=[], skip=0, retry=0):
        if retry > 10:
            return data
        try:
            # The string formating of strings woth curly braces in python is uggly {{ and }} instead of the simple one
            # I took the decision to use the old school % formating because its just one number and makes reading the query easier !
            # also, 100 is the max return number
            # spaces is a list like ["balancer", "yam.eth"]
            query = """
                query Votes {
                  votes (
                    first: 1000
                    skip:%s
                    where: {
                      proposal: "%s"
                    }
                  ) {
                    id
                    voter
                    created
                    choice
                    space {
                      id
                    }
                  }
                }
            """ % (
                skip,
                proposal_id,
            )

            query = gql(query.replace("'", '"'))

            result = self.client.execute(query)
            data += result["votes"]
            if len(result["votes"]) == 1000:
                time.sleep(0.5)
                return self.get_votes(proposal_id, data=data, skip=skip + 1000)
            return data
        except:
            time.sleep(retry + 1)
            return self.get_votes(proposal_id, data=data, skip=skip, retry=retry + 1)

    def get_proposals(self, spaces, data=[], skip=0, retry=0):
        if retry > 10:
            return data
        try:
            # The string formating of strings woth curly braces in python is uggly {{ and }} instead of the simple one
            # I took the decision to use the old school % formating because its just one number and makes reading the query easier !
            # also, 100 is the max return number
            # spaces is a list like ["balancer", "yam.eth"]
            query = """
                query Proposals {
                  proposals(
                    first: 100,
                    skip: %s,
                    where: {
                      space_in: %s,   
                      state: "closed"
                    },
                    orderBy: "created",
                    orderDirection: desc
                  ) {
                    id
                    title
                    body
                    choices
                    start
                    end
                    type
                    scores
                    scores_total
                    scores_state
                    scores_updated
                    snapshot
                    state
                    author
                    space {
                      id
                      name
                    }
                    votes
                  }
                }
            """ % (
                skip,
                spaces,
            )

            query = gql(query.replace("'", '"'))

            result = self.client.execute(query)
            data += result["proposals"]
            if len(result["proposals"]) == 100:
                time.sleep(0.5)
                return self.get_proposals(spaces, data=data, skip=skip + 100)
            return data
        except:
            time.sleep(retry + 1)
            return self.get_proposals(spaces, data=data, skip=skip, retry=retry + 1)

    def get_spaces(self, data=[], skip=0, retry=0):
        if retry > 10:
            return data
        try:
            # The string formating of strings woth curly braces in python is uggly {{ and }} instead of the simple one
            # I took the decision to use the old school % formating because its just one number and makes reading the query easier !
            # also, 100 is the max return number
            query = gql(
                """
                query Spaces {
                  spaces(
                    first: 100,
                    skip: %s,
                    orderBy: "created",
                    orderDirection: desc
                  ) {
                    id
                    name
                    about
                    network
                    symbol
                    strategies {
                      name
                      params
                    }
                    admins
                    members
                    filters {
                      minScore
                      onlyMembers
                    }
                    plugins
                  }
                }
            """
                % skip
            )

            result = self.client.execute(query)
            data += result["spaces"]
            if len(result["spaces"]) == 100:
                time.sleep(0.5)
                return self.get_spaces(data=data, skip=skip + 100)
            return data
        except:
            time.sleep(retry + 1)
            return self.get_spaces(data=data, skip=skip, retry=retry + 1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snapshot DAO Scrapper.")

    parser.add_argument(
        "-b",
        "--url",
        metavar="base_url",
        type=str,
        required=True,
        help="The base url for the Discourse forum to scrape data from, starting with http:// or https://",
    )
    parser.add_argument(
        "-o",
        "--out",
        metavar="out_path",
        type=str,
        required=True,
        help="The location for the folder containing the result of the scrapping.",
    )
    parser.add_argument(
        "-s",
        "--space",
        metavar="space_id",
        type=str,
        help="IF restricting to one space (DAO), the id of that DAO. For example: yam.eth",
    )

    args = parser.parse_args()

    scraper = SnapshotScapper(args.url, outpath=args.out)
    if args.space and args.space != "None":
        spaces = {"id": args.space}
        scraper.parse(spaces=[spaces])
    else:
        scraper.parse()
