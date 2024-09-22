import asyncio
import logging
from asyncio import CancelledError, Event, QueueEmpty, Task
from typing import Any, Literal

from .client import WSClient
from .client_config import WSClientConfig
from .eviction_policy import (
    EvictionPolicy,
    LatencyEvictionPolicy,
    MessageRateEvictionPolicy,
)
from .pool_queue import PoolQueue


class WSPool:
    """
    WSPool class for managing multiple WSClients.

    Args:
        pool_size (int)                                 : number of WSClients to use in the pool.
        url (str)                                       : ws url for the WSClients to connect to.
        eviction_policy (Literal["latency", "msg_rate"]): eviction policy to use.
        eviction_interval (int)                         : seconds of interval for eviction_policy to be applied.
        enable_ping (bool)                              : enable pinging on the clients.
        ping_interval (int)                             : if ping enabled set the interval of pings in seconds.
        sub_payload (list[dict[str, Any]] | None)       : optional payloads send on connection by the clients.
        name (str|None)                                 : optional name for logging, if not provided uses __name__.

    Attributes:
        name (str)                               : class name used for logging.
        pool_size (int)                          : number of clients in the pool.
        url (str)                                : url for the clients to use.
        data_queue (PoolQueue)                   : queue all clients will use to push messages, will only contain non duplicate messages and each item in queue is a tuple of bytearray and client_id.
        connected (asyncio.Event)                : event flag to indicate connection state.
        data_task (asyncio.Task)                 : task for handling/routing data from the pool_queue.
        enable_ping (bool)                       : enable pinging on the clients.
        ping_interval (int)                      : if ping enabled set the interval of pings in seconds.
        sub_payload (list[dict[str, Any]] | None): payloads send on connection by the clients.
        connections (dict[int, WSCLient])        : dict that holds reference to each client and their client id.
        eviction_policy (EvictionPolicy)         : eviction policy to use.
        eviction_interval (int)                  : seconds of interval for eviction_policy to be applied.
        eviction_task (asyncio.Task)             : task for periodically applying eviction policy.
    """

    __slots__ = (
        "name",
        "pool_size",
        "url",
        "data_queue",
        "connected",
        "data_task",
        "enable_ping",
        "ping_interval",
        "sub_payload",
        "connections",
        "eviction_policy",
        "eviction_interval",
        "eviction_task",
    )

    def __init__(
        self,
        pool_size: int,
        url: str,
        eviction_policy: Literal["latency", "msg_rate"],
        eviction_interval: int = 300,
        enable_ping: bool = True,
        ping_interval: int = 20,
        sub_payload: list[dict[str, Any]] | None = None,
        name: str | None = None,
    ) -> None:
        self.name: str = (
            f"{self.__class__.__name__}_{name}" if name else self.__class__.__name__
        )
        self.pool_size: int = pool_size
        self.url: str = url
        self.data_queue: PoolQueue = PoolQueue(queue_size=0, dedup_buffer_size=1000)
        self.connected: Event = Event()
        self.data_task: Task | None = None
        self.enable_ping: bool = enable_ping
        self.ping_interval: int = ping_interval
        self.sub_payload: list[dict[str, Any]] | None = sub_payload
        self.connections: dict[int, WSClient] = {}
        self.eviction_policy: EvictionPolicy = self._eviction_policy_factory(
            policy_type=eviction_policy
        )
        self.eviction_interval: int = eviction_interval
        self.eviction_task: Task | None = None

    def _eviction_policy_factory(
        self, policy_type: Literal["latency", "msg_rate"]
    ) -> EvictionPolicy:
        """
        Initialises the specified EvictionPolicy subclass.

        Args:
            policy_type (Literal["latency", "msg_rate"]).

        Returns:
            eviction_policy (EvictionPolicy): subclass to use.

        Raises:
            ValueError: unknown policy_type provided.
        """
        if policy_type == "latency":
            return LatencyEvictionPolicy()
        if policy_type == "msg_rate":
            return MessageRateEvictionPolicy()
        raise ValueError(f"{self.name} unknown policy type")

    def process_data(self, msg: bytearray, client_id: int) -> None:
        """Called for each message we get from the data_queue."""
        logging.debug(f"{self.name} client: {client_id}, msg: {msg}")

    async def handle_data(self) -> None:
        """
        Loop coro to get messages and client id out of the queue.
        Should be used to further direct data to their needed location/handlers. Drains the entire queue on each message and ONLY yields control back after QueueEmpty. Change to await get() instead if this causes issues.
        """
        try:
            await asyncio.wait_for(self.connected.wait(), timeout=3)

            while self.connected.is_set():
                try:
                    msg, client_id = self.data_queue.get_no_wait()
                    self.process_data(msg=msg, client_id=client_id)

                except QueueEmpty:
                    await asyncio.sleep(0)
                    continue

        except TimeoutError:
            logging.error(
                f"{self.name} timed out waiting for connected event in handle_data"
            )

        except Exception as e:
            logging.error(f"{self.name} unhandled exception in handle_data: {e}")

    async def start(self):
        """
        Starts the entire pool by initializing the clients and awaiting start() on them. and starts the handle_data coro as task.

        Raises:
            RuntimeError: if RuntimeError gets raised by a client start
            Exception: if unhandled exception gets raised by a client
        """

        self.connected.clear()
        for i in range(self.pool_size):
            self.connections[i] = WSClient(
                client_id=i,
                client_config=WSClientConfig(
                    url=self.url,
                    sub_payload=self.sub_payload,
                    enable_ping=self.enable_ping,
                    ping_interval=self.ping_interval,
                ),
                pool_queue=self.data_queue,
            )
        try:
            await asyncio.gather(*(conn.start() for conn in self.connections.values()))
        except RuntimeError as e:
            logging.error(
                f"{self.name} encountered RuntimeError in starting a client: {e}"
            )
            raise
        except Exception as e:
            logging.error(f"{self.name} unhandled exception in starting a client: {e}")
            raise
        self.connected.set()
        self.data_task = asyncio.create_task(
            self.handle_data(), name=f"{self.name}_handle_data"
        )
        self.eviction_task = asyncio.create_task(
            self.apply_eviction_policy(), name=f"{self.name}_eviction_task"
        )

    async def shutdown(self) -> None:
        """
        Shuts down the entire pool by awaitng shutdown() on each client and stops and cancels the handle_data task.
        """
        self.connected.clear()
        for conn in self.connections.values():
            await conn.shutdown()

        if self.data_task and not self.data_task.done():
            self.data_task.cancel(msg="cancel from shutdown")
            try:
                await self.data_task
            except CancelledError:
                pass

        if self.eviction_task and not self.eviction_task.done():
            self.eviction_task.cancel(msg="cancel from shutdown")
            try:
                await self.eviction_task
            except CancelledError:
                pass

    async def apply_eviction_policy(self) -> None:
        """Applies eviction policy to restart clients."""
        while self.connected.is_set():
            await asyncio.sleep(self.eviction_interval)
            if not self.connected.is_set():
                break

            restart_clients: dict[
                int, WSClient
            ] = await self.eviction_policy.clients_to_restart(
                connections=self.connections
            )
            logging.debug(f"{self.name} restarting: {restart_clients}")
            for client in restart_clients.values():
                await client.shutdown()
                await asyncio.sleep(0.5)
                try:
                    await client.start()
                except (RuntimeError, Exception) as e:
                    logging.error(
                        f"{self.name} {client.client_id} failed to start: {e}"
                    )
                    continue
