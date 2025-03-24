import json
import pathlib
from typing import Any, Dict, Optional

import datahub
from datahub import _version
from datahub.ingestion.graph.client import DataHubGraph
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import Filter, compile_filters
from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel

is_dev_mode = _version.is_dev_mode()
datahub_package_dir = pathlib.Path(datahub.__file__).parent.parent.parent

mcp = FastMCP(
    name="datahub",
    dependencies=[
        (
            # No spaces, since MCP doesn't escape their commands properly :(
            f"acryl-datahub@{datahub_package_dir}" if is_dev_mode else "acryl-datahub"
        ),
    ],
)


def get_client() -> DataHubClient:
    return DataHubClient.from_env()


search_gql = (pathlib.Path(__file__).parent / "gql/search.gql").read_text()
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()

query_fragment_gql = """
fragment platformFields on DataPlatform {
    urn
    type
    lastIngested
    name
    properties {
        type
        displayName
        datasetNameDelimiter
        logoUrl
    }
    displayName
    info {
        type
        displayName
        datasetNameDelimiter
        logoUrl
    }
}

fragment query on QueryEntity {
    urn
    properties {
        name
        description
        source
        statement {
            value
            language
        }
        created {
            time
            actor
        }
        lastModified {
            time
            actor
        }
    }
    platform {
        ...platformFields
    }
    subjects {
        dataset {
            urn
            type
            name
        }
        schemaField {
            urn
            type
            fieldPath
        }
    }
}
"""


@mcp.tool(description="Get an entity by its DataHub URN.")
def get_entity(urn: str) -> dict:
    client = get_client()

    # Create the GetEntity query using the fragment
    query = (
        entity_details_fragment_gql
        + """
    query entity($urn: String!) {
        entity(urn: $urn) {
            urn
            ...entityPreview
            ...entityDetails
        }
    }
    """
    )
    # Execute the GraphQL query
    variables = {"urn": urn}
    result = client._graph.execute_graphql(query=query, variables=variables)

    # Extract the entity data from the response
    if "entity" in result:
        return result["entity"]

    # Return empty dict if entity not found
    return {}


@mcp.tool(
    description="""Search across DataHub entities.

Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
To search for all entities, use the wildcard '*' as the query.

A typical workflow will involve multiple calls to this search tool, with each call refining the filters based on the facets/aggregations returned in the previous call.
After the final search is performed, you'll want to use the other tools to get more details about the relevant entities.

Here are some example filters:
- Production environment warehouse assets
```
{
  "and": [
    {"env": ["PROD"]},
    {"platform": ["snowflake", "bigquery", "redshift"]}
  ]
}
```

- All Snowflake tables
```
{
  "and_":[
    {"entity_type": ["DATASET"]},
    {"entity_type": "dataset", "entity_subtype": "Table"},
    {"platform": ["snowflake"]}
  ]
}
```
"""
)
def search(
    query: str = "*", filters: Optional[Filter] = None, num_results: int = 10
) -> str:
    client = get_client()

    # default_entity_types = [
    #     "container",
    #     "dataset",
    #     "dashboard",
    #     "chart",
    #     "dataJob",
    #     "dataFlow",
    # ]

    variables = {
        "query": query,
        # "types": client._graph._get_types(default_entity_types),  # TODO enable this? or rely on the backend's default?
        "orFilters": compile_filters(filters),
        "batchSize": num_results,
    }

    response = client._graph.execute_graphql(
        search_gql,
        variables=variables,
        operation_name="search",
    )

    # TODO: post process
    # e.g. strip all nulls?

    return response


@mcp.tool(description="Use this tool to get the queries associated with a dataset.")
def get_dataset_queries(dataset_urn: str, start: int = 0, count: int = 10) -> dict:
    client = get_client()

    # Create the ListQueries query using the fragment
    query = (
        query_fragment_gql
        + """
    query listQueries($input: ListQueriesInput!) {
        listQueries(input: $input) {
            start
            total
            count
            queries {
                ...query
            }
        }
    }
    """
    )

    # Set up variables for the query
    variables = {"input": {"start": start, "count": count, "datasetUrn": dataset_urn}}

    # Execute the GraphQL query
    result = client._graph.execute_graphql(query=query, variables=variables)

    # Extract the query data from the response
    if "listQueries" in result:
        return result["listQueries"]

    # Return empty dict if no queries found
    return {"start": start, "total": 0, "count": 0, "queries": []}


class AssetLineageDirective(BaseModel):
    urn: str
    upstream: bool
    downstream: bool
    num_hops: int


class AssetLineageAPI:
    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def get_degree_filter(self, num_hops: int) -> str:
        """
        num_hops: Number of hops to search for lineage
        """
        if num_hops < 1:
            return ""
        else:
            values = [str(i) for i in range(1, num_hops + 1)]
            return f"""
            orFilters:[{{and: {{ field: "degree", values: {json.dumps(values)} }} }}]
            """

    def get_final_gql(self, urn, direction, num_hops=1):
        return f"""{entity_hydration_fragment_gql}
    query {{
    searchAcrossLineage(input:{{
        urn: "{urn}",
        start:0,
        count:30,
        direction:{direction},
        {self.get_degree_filter(num_hops)}
    }}) {{
        total
        facets {{
        field
        displayName
        aggregations {{
            value
            count
            entity {{
            urn
            }}
        }}
        }}
        searchResults {{
        entity {{
            urn
            type
            ... entityPreview
        }}
        degree
        }}
    }}
    }}"""

    def get_lineage(
        self, asset_lineage_directive: AssetLineageDirective
    ) -> Dict[str, Any]:
        result = {asset_lineage_directive.urn: {}}
        if asset_lineage_directive.upstream:
            final_gql = self.get_final_gql(
                urn=asset_lineage_directive.urn,
                direction="UPSTREAM",
                num_hops=asset_lineage_directive.num_hops,
            )
            result[asset_lineage_directive.urn]["upstreams"] = (
                self.graph.execute_graphql(
                    query=final_gql,
                    variables={},
                )
            )
        if asset_lineage_directive.downstream:
            final_gql = self.get_final_gql(
                urn=asset_lineage_directive.urn,
                direction="DOWNSTREAM",
                num_hops=asset_lineage_directive.num_hops,
            )
            result[asset_lineage_directive.urn]["downstreams"] = (
                self.graph.execute_graphql(
                    query=final_gql,
                    variables={},
                )
            )

        return result


@mcp.tool(
    description="Use this tool to get upstream or downstream lineage for any entity.\
          Set upstream to True for upstream lineage, False for downstream lineage."
)
def get_lineage(urn: str, upstream: bool, num_hops: int = 1) -> dict:
    client = get_client()
    lineage_api = AssetLineageAPI(client._graph)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn, upstream=upstream, downstream=not upstream, num_hops=num_hops
    )
    return lineage_api.get_lineage(asset_lineage_directive)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        urn_or_query = sys.argv[1]
    else:
        urn_or_query = "*"
        print("No query provided, will use '*' query")
    if urn_or_query.startswith("urn:"):
        urn = urn_or_query
    else:
        urn = None
        query = urn_or_query
    if urn is None:
        search_data = search()
        for entity in search_data["scrollAcrossEntities"]["searchResults"]:
            print(entity["entity"]["urn"])
            urn = entity["entity"]["urn"]

    print("Getting entity:", urn)
    print(get_entity(urn))
    print("Getting lineage:", urn)
    print(get_lineage(urn, upstream=True))
    print("Getting queries", urn)
    print(get_dataset_queries(urn))
