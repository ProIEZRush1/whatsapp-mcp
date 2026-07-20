from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from whatsapp import (
    search_contacts as whatsapp_search_contacts,
    list_messages as whatsapp_list_messages,
    list_chats as whatsapp_list_chats,
    get_chat as whatsapp_get_chat,
    get_direct_chat_by_contact as whatsapp_get_direct_chat_by_contact,
    get_contact_chats as whatsapp_get_contact_chats,
    get_last_interaction as whatsapp_get_last_interaction,
    get_message_context as whatsapp_get_message_context,
    send_message as whatsapp_send_message,
    list_group_members as whatsapp_list_group_members,
    send_file as whatsapp_send_file,
    send_audio_message as whatsapp_audio_voice_message,
    download_media as whatsapp_download_media,
    query_database as whatsapp_query_database,
    send_reaction as whatsapp_send_reaction,
    mark_as_read as whatsapp_mark_as_read,
    send_presence as whatsapp_send_presence,
    subscribe_chat as whatsapp_subscribe_chat,
    unsubscribe_chat as whatsapp_unsubscribe_chat,
    list_subscriptions as whatsapp_list_subscriptions,
)

# Initialize FastMCP server
mcp = FastMCP("whatsapp")

@mcp.tool()
def search_contacts(query: str) -> Any:
    """Search WhatsApp contacts by name or phone number.
    
    Args:
        query: Search term to match against contact names or phone numbers
    """
    contacts = whatsapp_search_contacts(query)
    return contacts

@mcp.tool()
def list_messages(
    after: Optional[str] = None,
    before: Optional[str] = None,
    sender_phone_number: Optional[str] = None,
    chat_jid: Optional[str] = None,
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_context: bool = True,
    context_before: int = 1,
    context_after: int = 1
) -> str:
    """Get WhatsApp messages matching specified criteria with optional context.
    
    Args:
        after: Optional ISO-8601 formatted string to only return messages after this date
        before: Optional ISO-8601 formatted string to only return messages before this date
        sender_phone_number: Optional phone number to filter messages by sender
        chat_jid: Optional chat JID to filter messages by chat
        query: Optional search term to filter messages by content
        limit: Maximum number of messages to return (default 20)
        page: Page number for pagination (default 0)
        include_context: Whether to include messages before and after matches (default True)
        context_before: Number of messages to include before each match (default 1)
        context_after: Number of messages to include after each match (default 1)
    """
    messages = whatsapp_list_messages(
        after=after,
        before=before,
        sender_phone_number=sender_phone_number,
        chat_jid=chat_jid,
        query=query,
        limit=limit,
        page=page,
        include_context=include_context,
        context_before=context_before,
        context_after=context_after
    )
    return messages

@mcp.tool()
def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> Any:
    """Get WhatsApp chats matching specified criteria.
    
    Args:
        query: Optional search term to filter chats by name or JID
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
        include_last_message: Whether to include the last message in each chat (default True)
        sort_by: Field to sort results by, either "last_active" or "name" (default "last_active")
    """
    chats = whatsapp_list_chats(
        query=query,
        limit=limit,
        page=page,
        include_last_message=include_last_message,
        sort_by=sort_by
    )
    return chats

@mcp.tool()
def get_chat(chat_jid: str, include_last_message: bool = True) -> Any:
    """Get WhatsApp chat metadata by JID.
    
    Args:
        chat_jid: The JID of the chat to retrieve
        include_last_message: Whether to include the last message (default True)
    """
    chat = whatsapp_get_chat(chat_jid, include_last_message)
    return chat

@mcp.tool()
def get_direct_chat_by_contact(sender_phone_number: str) -> Any:
    """Get WhatsApp chat metadata by sender phone number.
    
    Args:
        sender_phone_number: The phone number to search for
    """
    chat = whatsapp_get_direct_chat_by_contact(sender_phone_number)
    return chat

@mcp.tool()
def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> Any:
    """Get all WhatsApp chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    chats = whatsapp_get_contact_chats(jid, limit, page)
    return chats

@mcp.tool()
def get_last_interaction(jid: str) -> str:
    """Get most recent WhatsApp message involving the contact.
    
    Args:
        jid: The JID of the contact to search for
    """
    message = whatsapp_get_last_interaction(jid)
    return message

@mcp.tool()
def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> Dict[str, Any]:
    """Get context around a specific WhatsApp message.
    
    Args:
        message_id: The ID of the message to get context for
        before: Number of messages to include before the target message (default 5)
        after: Number of messages to include after the target message (default 5)
    """
    context = whatsapp_get_message_context(message_id, before, after)
    return context

@mcp.tool()
def send_message(
    recipient: str,
    message: str,
    reply_to: Optional[str] = None,
    reply_to_sender: Optional[str] = None,
    mentions: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Send a WhatsApp message to a person or group. For group chats use the JID.
    Optionally reply to (quote) a specific message natively, and/or @-mention members.

    Mentioning members: write "@<number>" in the message text (number = international
    number, digits only, no +) for each person, e.g. "gracias @5215551091852". Those
    "@<number>" tokens are auto-detected and tagged, so the recipients see the member's
    NAME and get pinged. To mention someone WITHOUT writing their number in the visible
    text, pass their numbers/JIDs in `mentions`. Use list_group_members to resolve a
    member's name to the number to use.

    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        message: The message text to send
        reply_to: Optional ID of a message to quote/reply to. The quoted preview
                 (original text and sender) is looked up automatically from history.
        reply_to_sender: Optional JID of the quoted message's sender; only needed in
                 groups when auto-lookup can't resolve it.
        mentions: Optional list of numbers or JIDs to @-mention. Usually unnecessary —
                 "@<number>" tokens already in the message text are tagged automatically.

    Returns:
        A dictionary containing success status and a status message
    """
    # Validate input
    if not recipient:
        return {
            "success": False,
            "message": "Recipient must be provided"
        }

    success, status_message = whatsapp_send_message(
        recipient, message, reply_to=reply_to, reply_participant=reply_to_sender,
        mentions=mentions,
    )
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def list_group_members(group_jid: str) -> Dict[str, Any]:
    """List the members of a WhatsApp group so you can @-mention them.

    Returns each member's name and the phone number to use for a mention. To mention
    a member, write "@<phone>" in your message text (or pass the number in the
    send_message `mentions` argument).

    Args:
        group_jid: The group JID, e.g. "123456789@g.us"

    Returns:
        A dictionary with success status and, on success, a `members` list of
        {jid, phone, name, is_admin}.
    """
    success, result = whatsapp_list_group_members(group_jid)
    if success:
        return {"success": True, "members": result}
    return {"success": False, "message": result}

@mcp.tool()
def send_file(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send a file such as a picture, raw audio, video or document via WhatsApp to the specified recipient. For group messages use the JID.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the media file to send (image, video, document)
    
    Returns:
        A dictionary containing success status and a status message
    """
    
    # Call the whatsapp_send_file function
    success, status_message = whatsapp_send_file(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def send_audio_message(recipient: str, media_path: str) -> Dict[str, Any]:
    """Send any audio file as a WhatsApp audio message to the specified recipient. For group messages use the JID. If it errors due to ffmpeg not being installed, use send_file instead.
    
    Args:
        recipient: The recipient - either a phone number with country code but no + or other symbols,
                 or a JID (e.g., "123456789@s.whatsapp.net" or a group JID like "123456789@g.us")
        media_path: The absolute path to the audio file to send (will be converted to Opus .ogg if it's not a .ogg file)
    
    Returns:
        A dictionary containing success status and a status message
    """
    success, status_message = whatsapp_audio_voice_message(recipient, media_path)
    return {
        "success": success,
        "message": status_message
    }

@mcp.tool()
def download_media(message_id: str, chat_jid: str) -> Dict[str, Any]:
    """Download media from a WhatsApp message and get the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        A dictionary containing success status, a status message, and the file path if successful
    """
    file_path = whatsapp_download_media(message_id, chat_jid)
    
    if file_path:
        return {
            "success": True,
            "message": "Media downloaded successfully",
            "file_path": file_path
        }
    else:
        return {
            "success": False,
            "message": "Failed to download media"
        }

@mcp.tool()
def query_database(query: str, params: Optional[List[Any]] = None, limit: int = 200) -> Dict[str, Any]:
    """Run a READ-ONLY SQL query directly against the WhatsApp messages database.

    Reliable fallback for reading data when the higher-level tools get stuck.
    Only SELECT / WITH / PRAGMA / EXPLAIN are allowed. Tables:
      - chats(jid, name, last_message_time)
      - messages(id, chat_jid, sender, content, timestamp, is_from_me, media_type,
                 filename, url, quoted_id, quoted_sender, quoted_content)
    Use ? placeholders and pass `params` to avoid SQL injection.

    Args:
        query: A read-only SQL statement.
        params: Optional list of parameters for ? placeholders.
        limit: Max rows to return (default 200, capped at 1000).
    """
    return whatsapp_query_database(query, params=params, limit=limit)


@mcp.tool()
def send_reaction(chat_jid: str, message_id: str, emoji: str, sender: Optional[str] = None) -> Dict[str, Any]:
    """React to a WhatsApp message with an emoji (e.g. "👍", "❤️"). An empty emoji removes the reaction.

    Args:
        chat_jid: JID of the chat containing the message.
        message_id: ID of the message to react to.
        emoji: The emoji to react with; pass "" to remove your reaction.
        sender: Optional JID of the message's sender (auto-detected from history if omitted).
    """
    success, status_message = whatsapp_send_reaction(chat_jid, message_id, emoji, sender=sender)
    return {"success": success, "message": status_message}


@mcp.tool()
def mark_as_read(chat_jid: str, message_id: Optional[str] = None, sender: Optional[str] = None) -> Dict[str, Any]:
    """Mark a message as read (blue checks). If message_id is omitted, marks the chat's latest incoming message.

    Args:
        chat_jid: JID of the chat.
        message_id: Optional specific message ID; defaults to the latest incoming message.
        sender: Optional sender JID (auto-detected if omitted).
    """
    success, status_message = whatsapp_mark_as_read(chat_jid, message_id=message_id, sender=sender)
    return {"success": success, "message": status_message}


@mcp.tool()
def send_presence(chat_jid: str, state: str = "composing") -> Dict[str, Any]:
    """Show presence in a chat: typing ("composing"), recording audio ("recording"), or stop ("paused").

    Args:
        chat_jid: JID of the chat.
        state: One of "composing", "recording", "paused".
    """
    success, status_message = whatsapp_send_presence(chat_jid, state)
    return {"success": success, "message": status_message}


@mcp.tool()
def subscribe_to_chat(
    chat_jid: str,
    target: str,
    kind: str = "cmux",
    include_own: bool = False,
) -> Dict[str, Any]:
    """Subscribe to push notifications for NEW WhatsApp messages in a chat.

    When a new message arrives in `chat_jid`, the bridge delivers it to `target`:
      - kind="cmux": `target` is a cmux surface id; the message is injected as a
        submitted prompt into THAT session, so the agent there wakes and can act.
        To notify THIS session, first get its surface id by running in a shell:
            echo $CMUX_SURFACE_ID
        then pass that value as `target`.
      - kind="http": `target` is any URL; the bridge POSTs the message JSON to it.

    Args:
        chat_jid: Chat JID to watch (e.g. "123@s.whatsapp.net" or a group "..@g.us"),
                  or "*" for ALL chats.
        target: cmux surface id (kind="cmux") or webhook URL (kind="http").
        kind: "cmux" or "http". Default "cmux".
        include_own: If True, also deliver messages you send. Default False.
    """
    if not chat_jid or not target:
        return {"success": False, "message": "chat_jid and target are required"}
    return whatsapp_subscribe_chat(chat_jid, target, kind=kind, include_own=include_own)


@mcp.tool()
def unsubscribe_from_chat(chat_jid: str, target: str, kind: str = "cmux") -> Dict[str, Any]:
    """Remove a WhatsApp push subscription created with subscribe_to_chat.

    Args:
        chat_jid: The chat JID (or "*") used when subscribing.
        target: The same target (cmux surface id or URL) used when subscribing.
        kind: "cmux" or "http". Default "cmux".
    """
    return whatsapp_unsubscribe_chat(chat_jid, target, kind=kind)


@mcp.tool()
def list_subscriptions() -> Dict[str, Any]:
    """List all active WhatsApp push (webhook) subscriptions."""
    return whatsapp_list_subscriptions()


if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')