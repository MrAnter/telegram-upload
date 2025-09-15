import os
import time
import click

from telethon import TelegramClient, types
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import CreateForumTopicRequest
from telethon.tl.types import MessageService, DocumentAttributeFilename, DocumentAttributeSticker, DocumentAttributeAnimated, DocumentAttributeAudio, DocumentAttributeImageSize, MessageMediaWebPage

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


    async def _get_or_create_topic_id(self, group_entity, topic_name):
        """Gets the ID of a topic, creating it if it doesn't exist."""
        topic_id = None
        async for message in self.iter_messages(group_entity):
            if message.action and isinstance(message.action, types.MessageActionTopicCreate):
                if message.action.title == topic_name:
                    topic_id = message.id
                    click.echo(f"Found topic '{topic_name}' with ID: {topic_id}")
                    break

        if topic_id is None:
            click.echo(f"Topic '{topic_name}' not found. Creating it...")
            try:
                request = CreateForumTopicRequest(
                    channel=group_entity,
                    title=topic_name,
                )
                result = await self(request)

                for update in result.updates:
                    if isinstance(update, types.UpdateNewChannelMessage):
                        if isinstance(update.message.action, types.MessageActionTopicCreate):
                            topic_id = update.message.id
                            click.echo(f"Topic '{topic_name}' created with ID: {topic_id}")
                            break
            except Exception as e:
                click.echo(f"Error creating topic: {e}", err=True)
                return None
        return topic_id


    def forward_messages_from_chat(self, source_chat, destination_chat, files_only=False, topic_name=None):
        """
        Forwards new messages from a source chat to a destination, tracking forwarded messages.
        """

        destination_topic_id = None

        source_entity = self._resolve_entity_with_flood_wait(source_chat)
        if not source_entity:
            return 0

        destination_entity = self._resolve_entity_with_flood_wait(destination_chat)
        if not destination_entity:
            return 0

        final_topic_name = topic_name

        if topic_name == "":
            source_name = getattr(source_entity, 'title', None) or getattr(source_entity, 'username', None)
            if source_name:
                final_topic_name = source_name
                click.echo(f"Using source channel name '{final_topic_name}' as the topic name.")
            else:
                click.echo("Could not determine source channel name for topic. Aborting.", err=True)
                return 0

        if final_topic_name:
            destination_topic_id = self.loop.run_until_complete(self._get_or_create_topic_id(destination_entity, final_topic_name))
            if not destination_topic_id:
                click.echo("Could not find or create the topic. Aborting.", err=True)
                return 0

        chat_name = getattr(source_entity, 'title', getattr(source_entity, 'username', f"ID: {source_entity.id}"))
        click.echo(f"Fetching messages from '{chat_name}'...")

        messages_iter = self.iter_messages(source_entity)
        all_messages = list(messages_iter)

        all_messages = [m for m in all_messages if m.action is None]

        if files_only:
            all_messages = [
                m for m in all_messages if m.document and
                    not any(isinstance(attr, DocumentAttributeSticker) for attr in m.document.attributes) and
                    not any(isinstance(attr, DocumentAttributeAnimated) for attr in m.document.attributes) and
                    not any(isinstance(attr, DocumentAttributeAudio) for attr in m.document.attributes) and
                    not any(isinstance(attr, DocumentAttributeImageSize) for attr in m.document.attributes)
            ]

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
        for message_to_send in new_messages:
            while True:
                try:
                    if message_to_send.media and not isinstance(message_to_send.media, types.MessageMediaWebPage):
                        self.send_file(
                            entity=destination_entity,
                            file=message_to_send.media,
                            caption=message_to_send.text,
                            reply_to=destination_topic_id
                        )
                    elif message_to_send.text:
                        self.send_message(
                            entity=destination_entity,
                            message=message_to_send.text,
                            reply_to=destination_topic_id
                        )

                    ids_for_log = {f"{source_entity.id}:{message_to_send.id}"}
                    self._save_forwarded_ids(ids_for_log)
                    self.forwarded_ids.update(ids_for_log)
                    total_forwarded_in_session += 1

                    if total_forwarded_in_session % 100 == 0 or total_forwarded_in_session == len(new_messages):
                        click.echo(f"  - Sent {total_forwarded_in_session}/{len(new_messages)} messages...")

                    break

                except FloodWaitError as e:
                    click.echo(f"Flood wait error: sleeping for {e.seconds} seconds.", err=True)
                    time.sleep(e.seconds)
                except Exception as e:
                    click.echo(f"An error occurred while sending message {message_to_send.id}: {e}", err=True)
                    break

        return total_forwarded_in_session
