import math
import asyncio
import logging
import traceback
from typing import Dict, Union, AsyncGenerator

from pyrogram import Client, utils, raw
from pyrogram.session import Session, Auth
from pyrogram.errors import AuthBytesInvalid
from pyrogram.file_id import FileId, FileType, ThumbnailSource

from Adarsh.vars import Var
from Adarsh.bot import work_loads
from Adarsh.server.exceptions import FIleNotFound
from .file_properties import get_file_ids

MAX_CHUNK_SIZE = 524288  # Telegram API max allowed chunk size (512KB)
MAX_QUEUE_SIZE = 2       # For buffering one-ahead

class ByteStreamer:
    def __init__(self, client: Client):
        self.clean_timer = 30 * 60
        self.client: Client = client
        self.cached_file_ids: Dict[int, FileId] = {}
        asyncio.create_task(self.clean_cache())

    async def get_file_properties(self, message_id: int) -> FileId:
        if message_id not in self.cached_file_ids:
            await self.generate_file_properties(message_id)
            logging.debug(f"Cached file properties for message ID {message_id}")
        return self.cached_file_ids[message_id]

    async def generate_file_properties(self, message_id: int) -> FileId:
        file_id = await get_file_ids(self.client, Var.BIN_CHANNEL, message_id)
        if not file_id:
            logging.debug(f"Message with ID {message_id} not found")
            raise FIleNotFound
        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, client: Client, file_id: FileId) -> Session:
        if (media_session := client.media_sessions.get(file_id.dc_id)) is not None:
            logging.debug(f"Using cached media session for DC {file_id.dc_id}")
            return media_session

        test_mode = await client.storage.test_mode()
        dc_id = await client.storage.dc_id()
        if file_id.dc_id != dc_id:
            auth = await Auth(client, file_id.dc_id, test_mode).create()
            session = Session(client, file_id.dc_id, auth, test_mode, is_media=True)
            await session.start()
            for _ in range(6):
                exported = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=file_id.dc_id))
                try:
                    await session.send(raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
                    break
                except AuthBytesInvalid:
                    logging.debug(f"Invalid auth bytes for DC {file_id.dc_id}")
            else:
                await session.stop()
                raise AuthBytesInvalid
        else:
            session = Session(client, file_id.dc_id, await client.storage.auth_key(), test_mode, is_media=True)
            await session.start()

        logging.debug(f"Created media session for DC {file_id.dc_id}")
        client.media_sessions[file_id.dc_id] = session
        return session

    @staticmethod
    async def get_location(file_id: FileId) -> Union[
        raw.types.InputPhotoFileLocation,
        raw.types.InputDocumentFileLocation,
        raw.types.InputPeerPhotoFileLocation,
    ]:
        if file_id.file_type == FileType.CHAT_PHOTO:
            peer = raw.types.InputPeerUser(user_id=file_id.chat_id, access_hash=file_id.chat_access_hash) \
                if file_id.chat_id > 0 else \
                raw.types.InputPeerChat(chat_id=-file_id.chat_id) if file_id.chat_access_hash == 0 else \
                raw.types.InputPeerChannel(channel_id=utils.get_channel_id(file_id.chat_id), access_hash=file_id.chat_access_hash)
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer,
                volume_id=file_id.volume_id,
                local_id=file_id.local_id,
                big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        elif file_id.file_type == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )
        else:
            return raw.types.InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size,
            )

    async def yield_file(
        self,
        file_id: FileId,
        index: int,
        offset: int,
        first_part_cut: int,
        last_part_cut: int,
        part_count: int,
        chunk_size: int = MAX_CHUNK_SIZE,
    ) -> AsyncGenerator[bytes, None]:
        client = self.client
        work_loads[index] += 1
        current_part = 1
        queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

        async def fetch_chunks():
            nonlocal offset, current_part
            try:
                media_session = await self.generate_media_session(client, file_id)
                location = await self.get_location(file_id)
                while current_part <= part_count:
                    try:
                        r = await media_session.send(
                            raw.functions.upload.GetFile(
                                location=location, offset=offset, limit=chunk_size
                            )
                        )
                        if not isinstance(r, raw.types.upload.File):
                            break

                        chunk = r.bytes
                        if not chunk:
                            break

                        await queue.put((current_part, chunk))
                        offset += chunk_size
                        current_part += 1
                    except Exception as e:
                        logging.warning(f"Chunk fetch failed: {e}")
                        break
            except asyncio.CancelledError:
                logging.debug("fetch_chunks was cancelled.")
                raise
            finally:
                try:
                    await queue.put((None, None))  # Sentinel to stop consumer
                except Exception:
                    pass

        fetcher = asyncio.create_task(fetch_chunks())

        try:
            while True:
                part, chunk = await queue.get()
                if part is None:
                    break

                if part_count == 1:
                    yield chunk[first_part_cut:last_part_cut]
                elif part == 1:
                    yield chunk[first_part_cut:]
                elif part == part_count:
                    yield chunk[:last_part_cut]
                else:
                    yield chunk
        except Exception as e:
            logging.error(f"Streaming failed: {e}")
            logging.debug(traceback.format_exc())
        finally:
            fetcher.cancel()
            try:
                await fetcher
            except asyncio.CancelledError:
                pass
            work_loads[index] -= 1
            logging.debug(f"Finished high-speed streaming with {current_part - 1} parts.")

    async def clean_cache(self) -> None:
        while True:
            await asyncio.sleep(self.clean_timer)
            self.cached_file_ids.clear()
            logging.debug("Cache cleared.")
