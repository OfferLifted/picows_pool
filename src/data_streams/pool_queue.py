from asyncio import Queue
from typing import Literal


class PoolQueue:
    """
    Custom queue class to be used by a WSPool.

    Args:
        queue_size (int)       : size of the asyncio.Queue
        dedup_buffer_size (int): size of the deduplication set

    Attributes:
        name (str)                                  : name of class used for logging.
        queue (asyncio.Queue[tuple[bytearray, int]]): holds messages and WSClient id's.
        dedup_buffer_size (int)                     : size of the deduplication set.
        _buffer_set (set[int])                      : set used for buffering when dedup set reaches max size.
        hash_set (set[int])                         : dedup set holds hashes of messages.
    """

    __slots__ = "name", "queue", "dedup_buffer_size", "hash_set", "_buffer_set"

    def __init__(self, queue_size: int = 0, dedup_buffer_size: int = 1000) -> None:
        self.name: str = self.__class__.__name__
        self.queue: Queue[tuple[bytearray, int]] = Queue(maxsize=queue_size)
        self.dedup_buffer_size: int = dedup_buffer_size
        self.hash_set: set[int] = set()
        self._buffer_set: set[int] = set()

    def add_hash(self, hash: int) -> Literal[0, 1]:
        """
        Checks if hash is in set and manages dedup_buffer_size.

        Args:
            hash (int): hash of a message.

        Returns:
            0 if hash is in set, 1 if it's not (new).
        """
        if hash in self.hash_set:
            return 0
        else:
            if len(self.hash_set) >= self.dedup_buffer_size:
                self._buffer_set.add(hash)
                if len(self._buffer_set) >= 50:
                    self.hash_set = self._buffer_set
                    self._buffer_set = set()
            self.hash_set.add(hash)
            return 1

    async def put(self, msg: tuple[bytearray, int], hash: int) -> None:
        """
        Put an item into the queue if hash isn't in the set already.
        If the queue is full, wait until a free slot is available before adding item.

        Args:
            msg (tuple[bytearray, int]): bytearray of WS message and WSClient id.
            hash (int)                 : hash of a message.
        """
        if not self.add_hash(hash):
            return
        await self.queue.put(msg)

    def put_no_wait(self, msg: tuple[bytearray, int], hash: int) -> None:
        """
        Put an item into the queue without blocking if hash isn't in the set already.
        If no free slot is immediately available, raise QueueFull.

        Args:
            msg (tuple[bytearray, int]): bytearray of WS message and WSClient id.
            hash (int)                 : hash of a message.

        Raises:
            QueueFull: No free slot immediately available.
        """
        if not self.add_hash(hash):
            return
        self.queue.put_nowait(msg)

    async def get(self) -> tuple[bytearray, int]:
        """Remove and return an item from the queue.
        If queue is empty, wait until an item is available.

        Returns:
            item (tuple[bytearray, int]): message in bytearray and WSClient id of responsible client.
        """
        return await self.queue.get()

    def get_no_wait(self) -> tuple[bytearray, int]:
        """Remove and return an item from the queue.
        Return an item if one is immediately available, else raise QueueEmpty.

        Returns:
            item (tuple[bytearray, int]): message in bytearray and WSClient id of responsible client.

        Raises:
            QueueEmpty: no item immediately available.
        """
        return self.queue.get_nowait()
