import os
import time
import json
import click
from datetime import datetime

from telethon import TelegramClient, types
from telethon.errors import FloodWaitError
from telethon.tl.types import PeerChannel, PeerChat
from telethon.tl.functions.channels import CreateForumTopicRequest, GetForumTopicsRequest
from telethon.tl.types import MessageService, DocumentAttributeFilename, DocumentAttributeSticker, DocumentAttributeAnimated, DocumentAttributeAudio, DocumentAttributeImageSize, MessageMediaWebPage, ForumTopic

FORWARD_STATE_FILE = 'forward_state.json'


class TelegramForwardClient(TelegramClient):
    """A Telegram client focused on forwarding messages and tracking them."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state = self._load_state()

    # region State management

    def _load_state(self):
        if os.path.exists(FORWARD_STATE_FILE):
            try:
                with open(FORWARD_STATE_FILE, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                click.echo(f"Warning: Could not read {FORWARD_STATE_FILE}. Starting fresh.", err=True)
        return {'channels': {}, 'entity_cache': {}}

    def _save_state(self):
        with open(FORWARD_STATE_FILE, 'w') as f:
            json.dump(self._state, f, indent=2)

    # endregion

    # region Entity resolution

    def _resolve_entity_with_flood_wait(self, chat_identifier):
        """Resolves a chat entity, handling FloodWaitError by waiting and retrying."""
        while True:
            try:
                return self.get_entity(chat_identifier)
            except FloodWaitError as e:
                click.echo(f"Flood wait error: sleeping for {e.seconds} seconds.", err=True)
                time.sleep(e.seconds)
            except Exception as e:
                # For supergroup/channel numeric IDs,
                # Telethon needs a PeerChannel with the -100 prefix stripped.
                str_id = str(chat_identifier)
                if str_id.startswith('-100'):
                    try:
                        return self.get_entity(PeerChannel(int(str_id[4:])))
                    except FloodWaitError as fe:
                        click.echo(f"Flood wait error: sleeping for {fe.seconds} seconds.", err=True)
                        time.sleep(fe.seconds)
                        continue
                    except Exception:
                        pass
                click.echo(f"Error: Could not resolve chat entity '{chat_identifier}'. Details: {e}", err=True)
                return None

    @staticmethod
    def _is_external_identifier(key):
        """Returns True for user-provided identifiers (URLs, usernames) worth logging."""
        return isinstance(key, str) and (key.startswith('http') or key.startswith('t.me') or key.startswith('@'))

    def get_entity(self, entity):
        cache_key = str(entity)
        if cache_key in self._state['entity_cache']:
            cached_id = self._state['entity_cache'][cache_key]
            if self._is_external_identifier(cache_key):
                click.echo(f"[cache] '{cache_key}' → {cached_id}")
            try:
                return super().get_entity(PeerChannel(cached_id))
            except Exception:
                click.echo(f"[cache] '{cache_key}' stale, re-resolving...", err=True)
                del self._state['entity_cache'][cache_key]
        if self._is_external_identifier(cache_key):
            click.echo(f"[cache] '{cache_key}' not in cache, resolving via API...")
        result = super().get_entity(entity)
        self._cache_entity(cache_key, result)
        return result

    async def get_input_entity(self, peer):
        cache_key = str(peer)
        if cache_key in self._state['entity_cache']:
            cached_id = self._state['entity_cache'][cache_key]
            if self._is_external_identifier(cache_key):
                click.echo(f"[cache] '{cache_key}' → {cached_id}")
            try:
                return await super().get_input_entity(PeerChannel(cached_id))
            except Exception:
                click.echo(f"[cache] '{cache_key}' stale, re-resolving...", err=True)
                del self._state['entity_cache'][cache_key]
        if self._is_external_identifier(cache_key):
            click.echo(f"[cache] '{cache_key}' not in cache, resolving via API...")
        result = await super().get_input_entity(peer)
        if self._is_external_identifier(cache_key):
            entity_id = getattr(result, 'channel_id', None) or getattr(result, 'chat_id', None) or getattr(result, 'user_id', None)
            if entity_id and cache_key not in self._state['entity_cache']:
                self._state['entity_cache'][cache_key] = entity_id
                self._save_state()
                click.echo(f"[cache] '{cache_key}' added → {entity_id}")
        return result

    def _cache_entity(self, cache_key, entity):
        if not self._is_external_identifier(cache_key):
            return
        entity_id = getattr(entity, 'id', None)
        if entity_id and cache_key not in self._state['entity_cache']:
            self._state['entity_cache'][cache_key] = entity_id
            self._save_state()
            click.echo(f"[cache] '{cache_key}' added → {entity_id}")

    # endregion

    # region Topic handling

    async def _get_or_create_topic_id(self, group_entity, topic_name):
        """Gets the ID of a topic, creating it if it doesn't exist."""
        click.echo(f"Searching topic '{topic_name}'...")
        topic_id = None
        try:
            result = await self(GetForumTopicsRequest(
                channel=group_entity,
                q=topic_name,
                offset_date=0,
                offset_id=0,
                offset_topic=0,
                limit=100,
            ))
            for topic in result.topics:
                if isinstance(topic, ForumTopic) and topic.title == topic_name:
                    topic_id = topic.id
                    click.echo(f"Found topic '{topic_name}' with ID: {topic_id}")
                    break
        except Exception as e:
            click.echo(f"Warning: GetForumTopicsRequest failed ({e}), falling back to message scan.", err=True)
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

    # endregion

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

        channel_key = str(source_entity.id)
        channel_state = self._state['channels'].get(channel_key, {})
        last_run_timestamp = channel_state.get('last_run', 0.0)
        last_message_id = channel_state.get('last_message_id', 0)
        current_run_timestamp = time.time()
        click.echo(f"Checking for messages newer than: {datetime.fromtimestamp(last_run_timestamp)}")

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

        all_messages = list(self.iter_messages(source_entity))
        all_messages = [m for m in all_messages if m.action is None]

        if files_only:
            skip_attrs = (DocumentAttributeSticker, DocumentAttributeAnimated,
                          DocumentAttributeAudio, DocumentAttributeImageSize)
            all_messages = [
                m for m in all_messages if m.document and
                not any(isinstance(attr, skip_attrs) for attr in m.document.attributes)
            ]

        if not all_messages:
            click.echo(f"No messages found matching the criteria in '{chat_name}'.")
            return 0

        messages_to_process = []
        processed_count = 0
        for msg in all_messages:
            if msg.id > last_message_id:
                messages_to_process.append(msg)
            elif msg.edit_date and msg.edit_date.timestamp() > last_run_timestamp:
                click.echo(f"  - Detected edit for message {msg.id}. Queuing for re-sending.")
                messages_to_process.append(msg)
            else:
                processed_count += 1

        if processed_count > 0:
            click.echo(f"Skipping {processed_count} already processed and unchanged message(s).")

        if not messages_to_process:
            click.echo("No new or newly edited messages to send.")
            self._update_channel_state(channel_key, current_run_timestamp, last_message_id)
            return 0

        messages_to_process.reverse()

        click.echo(
            f"Found {len(messages_to_process)} new messages. Forwarding to '{getattr(destination_entity, 'title', destination_chat)}'...")

        total_forwarded_in_session = 0
        max_forwarded_id = last_message_id
        for message_to_send in messages_to_process:
            while True:
                try:
                    if message_to_send.media and not isinstance(message_to_send.media, types.MessageMediaWebPage):
                        file_name = ""
                        if hasattr(message_to_send.media, 'document') and hasattr(message_to_send.media.document, 'attributes'):
                            for attr in message_to_send.media.document.attributes:
                                if isinstance(attr, types.DocumentAttributeFilename):
                                    file_name = attr.file_name
                                    break

                        new_caption = f"{file_name}\n\n{message_to_send.text or ''}".strip()

                        self.send_file(
                            entity=destination_entity,
                            file=message_to_send.media,
                            caption=new_caption,
                            reply_to=destination_topic_id
                        )
                    elif message_to_send.text:
                        self.send_message(
                            entity=destination_entity,
                            message=message_to_send.text,
                            reply_to=destination_topic_id
                        )

                    if message_to_send.id > max_forwarded_id:
                        max_forwarded_id = message_to_send.id
                    total_forwarded_in_session += 1

                    if total_forwarded_in_session % 100 == 0 or total_forwarded_in_session == len(messages_to_process):
                        click.echo(f"  - Sent {total_forwarded_in_session}/{len(messages_to_process)} messages...")

                    break

                except FloodWaitError as e:
                    click.echo(f"Flood wait error: sleeping for {e.seconds} seconds.", err=True)
                    time.sleep(e.seconds)
                except Exception as e:
                    click.echo(f"An error occurred while sending message {message_to_send.id}: {e}", err=True)
                    break

        self._update_channel_state(channel_key, current_run_timestamp, max_forwarded_id)
        click.echo(f"\nSuccessfully processed {total_forwarded_in_session} messages. Run timestamp updated.")

        return total_forwarded_in_session

    def _update_channel_state(self, channel_key, last_run, last_message_id):
        self._state['channels'][channel_key] = {
            'last_run': last_run,
            'last_message_id': last_message_id,
        }
        self._save_state()
