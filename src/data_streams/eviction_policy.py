import asyncio
import logging
from abc import ABC, abstractmethod
from time import time

import numpy as np

from .client import WSClient


class EvictionPolicy(ABC):
    """
    EvictionPolicy base class for handling periodic eviction based on logic implemented in subclasses.

    Attributes:
        name (str): name of class used for logging.
    """

    def __init__(self) -> None:
        self.name: str = self.__class__.__name__

    @abstractmethod
    async def clients_to_restart(
        self, connections: dict[int, WSClient]
    ) -> dict[int, WSClient]:
        """
        Handles actual eviction logic.

        Args:
            conncections (dict[int, WSClient]): connections of the pool.

        Returns:
            restart_clients (dict[int, WSClient]): dict of clients to restart.
        """
        return connections


class LatencyEvictionPolicy(EvictionPolicy):
    """
    Eviction policy based on latency. Client with the highest latency will be restarted.
    """

    def __init__(self) -> None:
        super().__init__()

    async def clients_to_restart(
        self, connections: dict[int, WSClient]
    ) -> dict[int, WSClient]:
        """
        Handles eviction logic based on the mean of ping latency.
        Also restarts disconnected clients.

        Args:
            conncections (dict[int, WSClient]): connections of the pool.

        Returns:
            restart_clients (dict[int, WSClient]): dict of clients to restart.
        """
        restart_clients = {}
        worst_latency = -1
        worst_client = None
        worst_client_id = None
        for client_id, client in connections.items():
            try:
                if not client.connected.is_set():
                    restart_clients[client_id] = client
                    await asyncio.sleep(0)
                    continue

                if (
                    abs(client.pong_recv - client.last_ping) // 1_000_000_000
                    > 2 * client.ping_interval
                ):
                    logging.debug(
                        f"{self.name} stale connection detected for {client_id}, restarting."
                    )
                    restart_clients[client_id] = client
                    await asyncio.sleep(0)
                    continue

                mean_latency = np.mean(client.latencies)
                await asyncio.sleep(0)
                logging.debug(
                    f"{self.name} client: {client_id} mean_latency: {mean_latency}"
                )

                if mean_latency > worst_latency:
                    worst_latency = mean_latency
                    worst_client = client
                    worst_client_id = client_id

                await asyncio.sleep(0)

            except Exception:
                logging.info(f"{self.name} failed to get latency of client {client_id}")
                restart_clients[client_id] = client
                await asyncio.sleep(0)
                continue
        if worst_client_id is None or worst_client is None:
            logging.warning(f"{self.name} worst_client or worst_client_id not set.")
        restart_clients[worst_client_id] = worst_client

        return restart_clients


class MessageRateEvictionPolicy(EvictionPolicy):
    """
    Eviction policy based on messages per second received. Client with the lowest rate will be restarted.
    """

    def __init__(self) -> None:
        super().__init__()

    async def clients_to_restart(
        self, connections: dict[int, WSClient]
    ) -> dict[int, WSClient]:
        """
        Handles eviction logic based on the message per second rate. Also restarts disconnected clients.

        Args:
            conncections (dict[int, WSClient]): connections of the pool.

        Returns:
            restart_clients (dict[int, WSClient]): dict of clients to restart.
        """
        restart_clients = {}
        worst_rate = 999_999_999
        worst_client = None
        worst_client_id = None
        for client_id, client in connections.items():
            try:
                if not client.connected.is_set():
                    restart_clients[client_id] = client
                    await asyncio.sleep(0)
                    continue

                if (
                    abs(client.pong_recv - client.last_ping) // 1_000_000_000
                    > 2 * client.ping_interval
                ):
                    logging.debug(
                        f"{self.name} stale connection detected for {client_id}, restarting."
                    )
                    restart_clients[client_id] = client
                    await asyncio.sleep(0)
                    continue

                msg_rate = client.msg_seq / (time() - client.start_time)
                await asyncio.sleep(0)

                logging.debug(f"{self.name} client: {client_id} msg_rate: {msg_rate}")
                if msg_rate < worst_rate:
                    worst_rate = msg_rate
                    worst_client = client
                    worst_client_id = client_id

                await asyncio.sleep(0)

            except Exception:
                logging.warning(
                    f"{self.name} failed to get msg_rate of client {client_id}"
                )
                restart_clients[client_id] = client
                await asyncio.sleep(0)
                continue
        if not worst_client_id or not worst_client:
            logging.warning(f"{self.name} worst_client or worst_client_id not set.")
        restart_clients[worst_client_id] = worst_client

        return restart_clients
