import asyncio
import logging
from asyncio import CancelledError, Event, Task
from collections import deque
from time import time, time_ns

import orjson
import xxhash
from picows import WSCloseCode, WSFrame, WSListener, WSMsgType, WSTransport, ws_connect

from .client_config import WSClientConfig
from .pool_queue import PoolQueue


class WSClient(WSListener):
    """
    Websocket client class subclassing picows WSListener.

    Args:
        client_id (int)               : client id provided by the pool.
        client_config (WSClientConfig): config to use.
        pool_queue (PoolQueue)        : message queue provided by the pool.

    Attributes:
        client_id (int)                   : client id provided by the pool.
        name (str)                        : name of class used for logging.
        output_queue (PoolQueue)          : message queue provided by the pool.
        connected (asyncio.Event)         : event flag to indicate connection state.
        latencies (deque)                 : deque storing last 100 latency values.
        last_ping (float)                 : hold time of last sent ping.
        pong_recv (float)                 : hold time of last received pong.
        msg_seq (int)                     : holds amount of message the client has received.
        final_frame (bytearray)           : bytearray of a full message.
        transport (WSTransport)           : underlying transport of WSListener.
        wait_for_disconnect (asyncio.Task): task for awaiting disconnection.
        ping_task (asyncio.Task)          : task for handling pinging if enabled in config for config set interval.
        start_time (float)                : time of on_ws_connected being called, used for message per second rate.
    """

    def __init__(
        self,
        client_id: int,
        client_config: WSClientConfig,
        pool_queue: PoolQueue,
    ) -> None:
        super().__init__()
        self.apply_config(client_config=client_config)
        self.client_id: int = client_id
        self.name: str = f"{self.__class__.__name__}_{self.client_id}"
        self.output_queue: PoolQueue = pool_queue

        self.connected: Event = Event()
        self.latencies: deque = deque([], maxlen=100)
        self.last_ping: float = time_ns()
        self.pong_recv: float = time_ns()
        self.msg_seq: int = 0
        self.final_frame: bytearray = bytearray()
        self.transport: WSTransport | None = None
        self.wait_for_disconnect: Task | None = None
        self.ping_task: Task | None = None
        self.start_time: float = 0.0

    def apply_config(self, client_config: WSClientConfig) -> None:
        """
        Applies config provided by the pool.

        Args:
            client_config (WSClientConfig): config for client.
        """
        self.url = client_config["url"]
        self.sub_payload = client_config["sub_payload"]
        self.enable_ping = client_config["enable_ping"]
        self.ping_interval = client_config["ping_interval"]

    def on_ws_connected(self, transport: WSTransport) -> None:
        """
        Called on connection. Start wait_for_disconnect task and ping task if enabled. Also resets stats and sets start_time to current time.

        Args:
            transport (WSTransport): underlying picows transport.

        Raises:
            RuntimeError: underlying transport remains None.
        """
        self.transport = transport
        if not self.transport:
            raise RuntimeError(f"{self.name} transport not set in on_ws_connected")
        if self.sub_payload:
            for pl in self.sub_payload:
                try:
                    data = orjson.dumps(pl)
                    self.transport.send(WSMsgType.TEXT, data)
                except Exception as e:
                    logging.error(f"{self.name} failed to send sub payload: {pl}, {e}")
                    continue
        self.connected.set()
        self.start_time = time()
        self.reset_stats()

        logging.debug(f"{self.name} connected to '{self.url}'")

        if self.enable_ping:
            self.ping_task = asyncio.create_task(self.ping(), name=f"{self.name}_ping")

        self.wait_for_disconnect = asyncio.create_task(
            self.transport.wait_disconnected(), name=f"{self.name}_wait_for_disconnect"
        )

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        """
        Called on every received frame. Differentiates between text and ping/pong frames, stores ping latency.Concatenates text frames to final_frame until fin type is received. Pushes final_frame copy to output_queue and clear final_frame for reuse.

        Args:
            transport (WSTransport): underlying picows transport.
            frame (WSFrame): picows received frame.

        Raises:
            Exception: if anything goes wrong.
        """
        try:
            if (
                frame.msg_type == WSMsgType.TEXT
                or frame.msg_type == WSMsgType.CONTINUATION
            ):
                self.final_frame += frame.get_payload_as_memoryview()
                if not frame.fin:
                    return

                self.msg_seq += 1
                self.output_queue.put_no_wait(
                    msg=(self.final_frame.copy(), self.client_id),
                    hash=xxhash.xxh3_64_intdigest(self.final_frame),
                )
                self.final_frame.clear()
                return

            if frame.msg_type == WSMsgType.PONG:
                self.pong_recv = time_ns()
                self.latencies.append((self.pong_recv - self.last_ping) // 1_000_000)
                logging.debug(
                    f"client_id: {self.client_id} latency: {self.latencies[-1]}"
                )
                return
            if frame.msg_type == WSMsgType.CLOSE:
                transport.disconnect()
                self.transport.disconnect()  # JIC
                return

            else:
                logging.warning(
                    f"{self.name} received non-text frame: {frame.msg_type if frame else ''}"
                )

        except Exception as e:
            logging.error(f"{self.name} unhandled exception in on_ws_frame: {e}")

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        """
        Called on disconnection. Cancels running ping task if enabled. Clears connected state event.

        Args:
            transport (WSTransport): underlying picows transport.
        """
        self.connected.clear()
        logging.info(f"{self.name} disconnected from '{self.url}'")
        if not self.enable_ping:
            return
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel(msg="cancel from on_ws_disconnected")

    def send_bytes(self, data: bytes) -> None:
        """
        Sends bytes.

        Args:
            data (bytes): data to send in bytes.
        """
        if not self.transport:
            raise RuntimeError(f"{self.name} transport is None, cannot send bytes")
        try:
            self.transport.send(WSMsgType.TEXT, data)
        except Exception as e:
            logging.error(f"{self.name} failed to send data: {data}, {e}")
            return

    def send_ping(self) -> None:
        """
        Sends ping frame.

        Raises
            RuntimeError: transport is None so cannot send ping.
        """
        if not self.transport:
            raise RuntimeError(f"{self.name} transport is None, cannot send ping")
        try:
            self.transport.send_ping()
        except Exception as e:
            logging.error(f"{self.name} failed to send ping, {e}")
            return

    def send_json(self, data: dict) -> None:
        """
        Sends json.

        Args:
            data (dict): data to send as dict.

        Raises
            RuntimeError: transport is None so cannot send json.
        """
        if not self.transport:
            raise RuntimeError(f"{self.name} transport is None, cannot send json")
        try:
            self.transport.send(WSMsgType.TEXT, orjson.dumps(data))
        except Exception as e:
            logging.error(f"{self.name} failed to send json: {data}, {e}")
            return

    async def ping(self) -> None:
        """
        Coro used for ping task if enabled in config. Uses interval specific in config. Sets last_ping as current time for latency meassure.
        """
        try:
            await asyncio.wait_for(self.connected.wait(), timeout=3)
        except TimeoutError:
            logging.error(f"{self.name} ping task timed out waiting on connected event")
            return
        while self.connected.is_set():
            await asyncio.sleep(self.ping_interval)
            if not self.connected.is_set():
                break
            self.send_ping()
            self.last_ping = time_ns()

    async def start(self) -> None:
        """
        Starts the WSClient. Cancels prior wait_for_disconnect and ping tasks (they are restarted in on_ws_connected). Waits for connected state event to be set with timeout and calls shutdown on timeout.
        Raises:
            RuntimeError: underlying transport remains None.
            Exception   : if anything goes wrong.
        """
        try:
            if self.wait_for_disconnect and not self.wait_for_disconnect.done():
                logging.warning(
                    f"{self.name} prior wait_for_disconnect Task is not done on new start"
                )
                self.wait_for_disconnect.cancel(msg="cancel from start")
                try:
                    await self.wait_for_disconnect
                except CancelledError:
                    pass
                self.wait_for_disconnect = None

            if self.enable_ping and self.ping_task and not self.ping_task.done():
                logging.warning(f"{self.name} prior ping Task is not done on new start")
                self.ping_task.cancel(msg="cancel from start")
                try:
                    await self.ping_task
                except CancelledError:
                    pass
                self.ping_task = None

            logging.info(f"{self.name} starting connection for '{self.url}'")

            transport, _ = await ws_connect(lambda: self, self.url)

            if not transport or not self.transport:
                raise ConnectionError(
                    f"{self.name} underlying transport is None, cannot continue connection"
                )

            try:
                await asyncio.wait_for(self.connected.wait(), timeout=3)
            except TimeoutError:
                logging.error(f"{self.name} connecting timed out")
                await self.shutdown()
                return

        except Exception as e:
            logging.error(f"{self.name} unhandled exception in start: {e}")
            return

    async def shutdown(self) -> None:
        """
        Stops and cleans up the WSClient. Clears connected state event, cancels ping task if enabled, closes underlying transport and awaits wait_for_disconnect. Sets transport to None afterwards.
        """
        self.connected.clear()
        logging.info(f"{self.name} shutting down at '{self.url}'")
        if self.enable_ping:
            if self.ping_task and not self.ping_task.done():
                self.ping_task.cancel(msg="cancel from shutdown")
                try:
                    await self.ping_task
                except CancelledError:
                    pass
                self.ping_task = None

        if self.transport:
            self.transport.send_close(WSCloseCode.OK)
            self.transport.disconnect()
            if self.wait_for_disconnect:
                await self.wait_for_disconnect
            else:
                await self.transport.wait_disconnected()
            self.transport = None

    def reset_latencies(self) -> None:
        """Resets the latency deque to empty."""
        self.latencies = deque([], maxlen=100)

    def reset_msg_seq(self) -> None:
        """Resets msg_seq to 0."""
        self.msg_seq = 0

    def reset_stats(self) -> None:
        """Resets both latency deque and msg_seq."""
        self.latencies = deque([], maxlen=100)
        self.msg_seq = 0
        self.last_ping = time_ns()
        self.pong_recv = time_ns()
