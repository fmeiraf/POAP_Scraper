import requests
import time
import json

import pdb


def extract_token_nested_fields(target_obj: dict):
    """
    Extract nested objects from the token graphql query
    """
    new_dict = {}
    for key in target_obj.keys():
        if key in ["event", "owner"]:
            for nested_key in target_obj[key].keys():
                new_dict[f"{key}_{nested_key}"] = target_obj[key][nested_key]
        else:
            new_dict[f"token_{key}"] = target_obj[key]

    return new_dict


def extract_all_tokens_from_subgraph(
    subgraph_api_url: str, page_size: int, last_timestamp: int = 0
):

    """
    Extract all token data from subgraph.
    """

    last_timestamp = last_timestamp
    wait_time_seconds = 5
    pages_ran = 0
    extracted_data = []

    # pdb.set_trace()

    query = """
            query get_token($last_timestamp: Int, $page_size: Int) {
                tokens (first: $page_size, 
                        orderBy:id,
                        orderDirection: asc,
                        where: {created_gt: $last_timestamp}) 
                {
                    id
                    owner{
                        id
                    }
                    event {
                        id
                        tokenCount
                        created
                        transferCount
                    }
                    created
                    transferCount
                }
            }
        """

    print("Starting subgraph extraction..")
    while True:

        req = requests.post(
            subgraph_api_url,
            json={
                "query": query,
                "variables": {"last_timestamp": last_timestamp, "page_size": page_size},
            },
        )

        if req.status_code != 200:
            print(
                f"There was a problem with the request for last_timestamp:{last_timestamp}.\n Trying again in {wait_time_seconds}  seconds.. "
            )
            time.sleep(wait_time_seconds)
            continue

        j = json.loads(req.text)

        if not j["data"]["tokens"]:
            print("Subgraph extraction is DONE. \n")
            return extracted_data

        for token_data in j["data"]["tokens"]:
            cleaned_token_data = extract_token_nested_fields(token_data)
            extracted_data.append(cleaned_token_data)
            last_timestamp = int(cleaned_token_data["token_created"])

        pages_ran += 1
