from datahub.sdk.main_client import DataHubClient

from mcp_server_datahub.mcp_server import mcp, set_client


def main() -> None:
    set_client(DataHubClient.from_env())
    mcp.run()


if __name__ == "__main__":
    main()
