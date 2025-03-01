import pathlib
from typing import Optional

import datahub
from datahub import _version
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import Filter
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


@mcp.tool(description="Get an entity by its DataHub URN.")
def get_entity(urn: str) -> dict:
    client = get_client()

    # TODO: Migrate to new sdk
    # return client.entities.get(urn)

    # TODO: strip out useless aspects / fields?
    return client._graph.get_entity_raw(urn)


@mcp.tool(
    description="Search across DataHub entities. \
Returns both a truncated list of results \
and facets/aggregations that can be used to iteratively refine the search filters."
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

    graphql_query = """\
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
          urn
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

    variables = {
        "query": query,
        "types": client._graph._get_types(default_entity_types),  # TODO keep this?
        "orFilters": client.search._compile_filters(filters),
        "batchSize": 10,
    }

    response = client._graph.execute_graphql(graphql_query, variables)

    # TODO: post process
    # e.g. strip all nulls?

    return response
