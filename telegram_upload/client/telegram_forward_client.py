import os
import time
import click

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.tl.types import MessageService

FORWARDED_LOG_FILE = 'forwarded_messages.log'


class TelegramForwardClient(TelegramClient):
    """A Telegram client focused on forwarding messages and tracking them."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.forwarded_ids = self._load_forwarded_ids()
        click.echo(f"Loaded {len(self.forwarded_ids)} previously forwarded message IDs from '{FORWARDED_LOG_FILE}'.")

    def _load_forwarded_ids(self):
        """Loads all forwarded message IDs from the log file into a set."""
        if not os.path.exists(FORWARDED_LOG_FILE):
            return set()
        with open(FORWARDED_LOG_FILE, 'r') as f:
            return {line.strip() for line in f if line.strip()}

    def _save_forwarded_ids(self, ids_to_save):
        """Appends a set of successfully forwarded message IDs to the log file."""
        with open(FORWARDED_LOG_FILE, 'a') as f:
            for msg_id in ids_to_save:
                f.write(f"{msg_id}\n")


    def _resolve_entity_with_flood_wait(self, chat_identifier):
        """Resolves a chat entity, handling FloodWaitError by waiting and retrying."""

        while True:
            try:
                entity = self.get_entity(chat_identifier)
                return entity
            except FloodWaitError as e:
                click.echo(f"Flood wait error: sleeping for {e.seconds} seconds.", err=True)
                time.sleep(e.seconds)
            except Exception as e:
                click.echo(f"Error: Could not resolve chat entity '{chat_identifier}'. Details: {e}", err=True)
                return None


    def forward_messages_from_chat(self, source_chat, destination_chat, files_only=False):
        """
        Forwards new messages from a source chat to a destination, tracking forwarded messages.
        """

        source_entity = self._resolve_entity_with_flood_wait(source_chat)
        if not source_entity:
            return 0

        destination_entity = self._resolve_entity_with_flood_wait(destination_chat)
        if not destination_entity:
            return 0

        chat_name = getattr(source_entity, 'title', getattr(source_entity, 'username', f"ID: {source_entity.id}"))
        click.echo(f"Fetching messages from '{chat_name}'...")

        messages_iter = self.iter_messages(source_entity)
        all_messages = list(messages_iter)

        all_messages = [m for m in all_messages if m.action is None]

        if files_only:
            all_messages = [m for m in all_messages if m.media]

        if not all_messages:
            click.echo(f"No messages found matching the criteria in '{chat_name}'.")
            return 0

        new_messages = [
            m for m in all_messages
            if f"{source_entity.id}:{m.id}" not in self.forwarded_ids
        ]

        skipped_count = len(all_messages) - len(new_messages)
        if skipped_count > 0:
            click.echo(f"Skipping {skipped_count} already forwarded message(s).")

        if not new_messages:
            click.echo(f"No new messages to forward from '{chat_name}'.")
            return 0

        new_messages.reverse()

        chunk_size = 100
        message_chunks = [new_messages[i:i + chunk_size] for i in range(0, len(new_messages), chunk_size)]

        click.echo(
            f"Found {len(new_messages)} new messages. Forwarding to '{getattr(destination_entity, 'title', destination_chat)}' in {len(message_chunks)} chunk(s)...")

        total_forwarded_in_session = 0
        for chunk_index, message_chunk in enumerate(message_chunks):
            while True:
                try:
                    self.forward_messages(
                        entity=destination_entity,
                        messages=message_chunk,
                        from_peer=source_entity
                    )

                    ids_for_log = {f"{source_entity.id}:{m.id}" for m in message_chunk}
                    self._save_forwarded_ids(ids_for_log)
                    self.forwarded_ids.update(ids_for_log)

                    total_forwarded_in_session += len(message_chunk)
                    click.echo(f"  - Chunk {chunk_index + 1}/{len(message_chunks)} forwarded successfully.")
                    break
                except FloodWaitError as e:
                    click.echo(f"Flood wait error: sleeping for {e.seconds} seconds.", err=True)
                    time.sleep(e.seconds)
                except Exception as e:
                    click.echo(f"An error occurred while forwarding chunk {chunk_index + 1}: {e}", err=True)
                    break

        return total_forwarded_in_session
