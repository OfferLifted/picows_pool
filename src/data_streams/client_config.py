from typing import Any, TypedDict


class WSClientConfig(TypedDict):
    """
    Config dict for WSClient

    Args:
        url (str)                                : ws url to connect to.
        sub_payload (list[dict[str, Any]] | None): list of dictionaries of payloads send on connect.
        enable_ping (bool)                       : flag to enable pinging.
        ping_interval (int)                      : ping interval in seconds to use if ping is enabled.
    """

    url: str
    sub_payload: list[dict[str, Any]] | None
    enable_ping: bool
    ping_interval: int
