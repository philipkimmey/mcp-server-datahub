import pathlib
from typing import Optional

import datahub
from datahub import _version
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import Filter, compile_filters
from mcp.server.fastmcp import FastMCP

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


entity_hydration_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/datahub_semantic_layer.gql"
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
        entity_hydration_fragment_gql
        + """
    query entity($urn: String!) {
        entity(urn: $urn) {
            urn
            ...entityPreview
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
    description="Search across DataHub entities. \
Returns both a truncated list of results \
and facets/aggregations that can be used to iteratively refine the search filters. \
Here are some example filters: \
{ 'and': [ { 'entity_types': ['DATASET']}, { 'entity_subtypes' : ['Table']}]} \
"
)
def search(query: str = "*", filters: Optional[Filter] = None) -> str:
    client = get_client()

    default_entity_types = [
        "container",
        "dataset",
        "dashboard",
        "chart",
        "dataJob",
        "dataFlow",
    ]

    graphql_query = (
        entity_hydration_fragment_gql
        + """\
fragment FacetEntityInfo on Entity {
  ... on Dataset {
    name
    properties {
      name
    }
  }
  ... on Container {
    subTypes {
      typeNames
    }
    properties {
      name
    }
  }
  ... on GlossaryTerm {
    properties {
      name
    }
  }
}

query scrollUrnsWithFilters(
    $types: [EntityType!],
    $query: String!,
    $orFilters: [AndFilterInput!],
    $batchSize: Int!,
    $scrollId: String) {

    scrollAcrossEntities(input: {
        query: $query,
        count: $batchSize,
        scrollId: $scrollId,
        types: $types,
        orFilters: $orFilters,
        searchFlags: {
            skipHighlighting: true
            # skipAggregates: true
            maxAggValues: 5
        }
    }) {
      count
      total
      searchResults {
        entity {
          ...entityPreview
        }
      }
      facets {
        field
        displayName
        aggregations {
          value
          count
          displayName
          entity {
            ...FacetEntityInfo
          }
        }
      }
    }
}
"""
    )

    variables = {
        "query": query,
        "types": client._graph._get_types(default_entity_types),  # TODO keep this?
        "orFilters": compile_filters(filters),
        "batchSize": 10,
    }

    response = client._graph.execute_graphql(graphql_query, variables)

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


if __name__ == "__main__":
    urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,digital_market_hway.reporting.digital_media_performance,PROD)"
    print(get_entity(urn))
    print(search("data"))
    print(get_dataset_queries(urn))
