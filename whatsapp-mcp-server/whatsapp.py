import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple, Any
import os.path
import requests
import json
import audio

MESSAGES_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'whatsapp-bridge', 'store', 'messages.db')
WHATSAPP_API_BASE_URL = "http://localhost:8080/api"

# Network timeout (connect, read) for every call to the Go bridge so a slow or
# unresponsive bridge can never hang the MCP forever.
BRIDGE_TIMEOUT = (5, 60)


def _connect_db():
    """Open the messages DB read-only with a busy timeout.

    The Go bridge writes incoming messages constantly; opening read-only with a
    busy_timeout means the MCP never blocks (or hangs) indefinitely on a writer
    lock — at worst it waits a few seconds and raises, instead of getting stuck.
    """
    uri = f"file:{os.path.abspath(MESSAGES_DB_PATH)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10)
    try:
        conn.execute("PRAGMA busy_timeout=8000")
    except sqlite3.Error:
        pass
    return conn

@dataclass
class Message:
    timestamp: datetime
    sender: str
    content: str
    is_from_me: bool
    chat_jid: str
    id: str
    chat_name: Optional[str] = None
    media_type: Optional[str] = None
    quoted_id: Optional[str] = None
    quoted_sender: Optional[str] = None
    quoted_content: Optional[str] = None

@dataclass
class Chat:
    jid: str
    name: Optional[str]
    last_message_time: Optional[datetime]
    last_message: Optional[str] = None
    last_sender: Optional[str] = None
    last_is_from_me: Optional[bool] = None

    @property
    def is_group(self) -> bool:
        """Determine if chat is a group based on JID pattern."""
        return self.jid.endswith("@g.us")

@dataclass
class Contact:
    phone_number: str
    name: Optional[str]
    jid: str

@dataclass
class MessageContext:
    message: Message
    before: List[Message]
    after: List[Message]

def get_sender_name(sender_jid: str) -> str:
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        # First try matching by exact JID
        cursor.execute("""
            SELECT name
            FROM chats
            WHERE jid = ?
            LIMIT 1
        """, (sender_jid,))
        
        result = cursor.fetchone()
        
        # If no result, try looking for the number within JIDs
        if not result:
            # Extract the phone number part if it's a JID
            if '@' in sender_jid:
                phone_part = sender_jid.split('@')[0]
            else:
                phone_part = sender_jid
                
            cursor.execute("""
                SELECT name
                FROM chats
                WHERE jid LIKE ?
                LIMIT 1
            """, (f"%{phone_part}%",))
            
            result = cursor.fetchone()
        
        if result and result[0]:
            return result[0]
        else:
            return sender_jid
        
    except sqlite3.Error as e:
        print(f"Database error while getting sender name: {e}")
        return sender_jid
    finally:
        if 'conn' in locals():
            conn.close()

def format_message(message: Message, show_chat_info: bool = True) -> None:
    """Print a single message with consistent formatting."""
    output = ""
    
    if show_chat_info and message.chat_name:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] Chat: {message.chat_name} "
    else:
        output += f"[{message.timestamp:%Y-%m-%d %H:%M:%S}] "
        
    content_prefix = ""
    if hasattr(message, 'media_type') and message.media_type:
        content_prefix = f"[{message.media_type} - Message ID: {message.id} - Chat JID: {message.chat_jid}] "

    # If this message is a reply, show the quoted message it points to.
    quote_prefix = ""
    if getattr(message, 'quoted_content', None):
        try:
            quoted_sender_name = get_sender_name(message.quoted_sender) if message.quoted_sender else "?"
        except Exception:
            quoted_sender_name = message.quoted_sender or "?"
        quoted_text = (message.quoted_content or "").replace("\n", " ")
        if len(quoted_text) > 200:
            quoted_text = quoted_text[:200] + "…"
        quote_prefix = f"↩ Replying to {quoted_sender_name} (msg {message.quoted_id}): \"{quoted_text}\" | "

    try:
        sender_name = get_sender_name(message.sender) if not message.is_from_me else "Me"
        output += f"From: {sender_name}: {quote_prefix}{content_prefix}{message.content}\n"
    except Exception as e:
        print(f"Error formatting message: {e}")
    return output

def format_messages_list(messages: List[Message], show_chat_info: bool = True) -> None:
    output = ""
    if not messages:
        output += "No messages to display."
        return output
    
    for message in messages:
        output += format_message(message, show_chat_info)
    return output

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
) -> List[Message]:
    """Get messages matching the specified criteria with optional context."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        # Build base query
        query_parts = ["SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_id, messages.quoted_sender, messages.quoted_content FROM messages"]
        query_parts.append("JOIN chats ON messages.chat_jid = chats.jid")
        where_clauses = []
        params = []
        
        # Add filters
        if after:
            try:
                after = datetime.fromisoformat(after)
            except ValueError:
                raise ValueError(f"Invalid date format for 'after': {after}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp > ?")
            params.append(after)

        if before:
            try:
                before = datetime.fromisoformat(before)
            except ValueError:
                raise ValueError(f"Invalid date format for 'before': {before}. Please use ISO-8601 format.")
            
            where_clauses.append("messages.timestamp < ?")
            params.append(before)

        if sender_phone_number:
            where_clauses.append("messages.sender = ?")
            params.append(sender_phone_number)
            
        if chat_jid:
            where_clauses.append("messages.chat_jid = ?")
            params.append(chat_jid)
            
        if query:
            where_clauses.append("LOWER(messages.content) LIKE LOWER(?)")
            params.append(f"%{query}%")
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add pagination
        offset = page * limit
        query_parts.append("ORDER BY messages.timestamp DESC")
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        messages = cursor.fetchall()
        
        result = []
        for msg in messages:
            message = Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7],
                quoted_id=msg[8],
                quoted_sender=msg[9],
                quoted_content=msg[10]
            )
            result.append(message)
            
        if include_context and result:
            # Add context for each message
            messages_with_context = []
            for msg in result:
                context = get_message_context(msg.id, context_before, context_after)
                messages_with_context.extend(context.before)
                messages_with_context.append(context.message)
                messages_with_context.extend(context.after)
            
            return format_messages_list(messages_with_context, show_chat_info=True)
            
        # Format and display messages without context
        return format_messages_list(result, show_chat_info=True)    
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_message_context(
    message_id: str,
    before: int = 5,
    after: int = 5
) -> MessageContext:
    """Get context around a specific message."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        # Get the target message first
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.chat_jid, messages.media_type, messages.quoted_id, messages.quoted_sender, messages.quoted_content
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.id = ?
        """, (message_id,))
        msg_data = cursor.fetchone()

        if not msg_data:
            raise ValueError(f"Message with ID {message_id} not found")

        target_message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[8],
            quoted_id=msg_data[9],
            quoted_sender=msg_data[10],
            quoted_content=msg_data[11]
        )
        
        # Get messages before
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_id, messages.quoted_sender, messages.quoted_content
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp < ?
            ORDER BY messages.timestamp DESC
            LIMIT ?
        """, (msg_data[7], msg_data[0], before))
        
        before_messages = []
        for msg in cursor.fetchall():
            before_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7],
                quoted_id=msg[8],
                quoted_sender=msg[9],
                quoted_content=msg[10]
            ))
        
        # Get messages after
        cursor.execute("""
            SELECT messages.timestamp, messages.sender, chats.name, messages.content, messages.is_from_me, chats.jid, messages.id, messages.media_type, messages.quoted_id, messages.quoted_sender, messages.quoted_content
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ? AND messages.timestamp > ?
            ORDER BY messages.timestamp ASC
            LIMIT ?
        """, (msg_data[7], msg_data[0], after))
        
        after_messages = []
        for msg in cursor.fetchall():
            after_messages.append(Message(
                timestamp=datetime.fromisoformat(msg[0]),
                sender=msg[1],
                chat_name=msg[2],
                content=msg[3],
                is_from_me=msg[4],
                chat_jid=msg[5],
                id=msg[6],
                media_type=msg[7],
                quoted_id=msg[8],
                quoted_sender=msg[9],
                quoted_content=msg[10]
            ))
        
        return MessageContext(
            message=target_message,
            before=before_messages,
            after=after_messages
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def list_chats(
    query: Optional[str] = None,
    limit: int = 20,
    page: int = 0,
    include_last_message: bool = True,
    sort_by: str = "last_active"
) -> List[Chat]:
    """Get chats matching the specified criteria."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        # Build base query
        query_parts = ["""
            SELECT 
                chats.jid,
                chats.name,
                chats.last_message_time,
                messages.content as last_message,
                messages.sender as last_sender,
                messages.is_from_me as last_is_from_me
            FROM chats
        """]
        
        if include_last_message:
            query_parts.append("""
                LEFT JOIN messages ON chats.jid = messages.chat_jid 
                AND chats.last_message_time = messages.timestamp
            """)
            
        where_clauses = []
        params = []
        
        if query:
            where_clauses.append("(LOWER(chats.name) LIKE LOWER(?) OR chats.jid LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
            
        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))
            
        # Add sorting
        order_by = "chats.last_message_time DESC" if sort_by == "last_active" else "chats.name"
        query_parts.append(f"ORDER BY {order_by}")
        
        # Add pagination
        offset = (page ) * limit
        query_parts.append("LIMIT ? OFFSET ?")
        params.extend([limit, offset])
        
        cursor.execute(" ".join(query_parts), tuple(params))
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def search_contacts(query: str) -> List[Contact]:
    """Search contacts by name or phone number."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        # Split query into characters to support partial matching
        search_pattern = '%' +query + '%'
        
        cursor.execute("""
            SELECT DISTINCT 
                jid,
                name
            FROM chats
            WHERE 
                (LOWER(name) LIKE LOWER(?) OR LOWER(jid) LIKE LOWER(?))
                AND jid NOT LIKE '%@g.us'
            ORDER BY name, jid
            LIMIT 50
        """, (search_pattern, search_pattern))
        
        contacts = cursor.fetchall()
        
        result = []
        for contact_data in contacts:
            contact = Contact(
                phone_number=contact_data[0].split('@')[0],
                name=contact_data[1],
                jid=contact_data[0]
            )
            result.append(contact)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_contact_chats(jid: str, limit: int = 20, page: int = 0) -> List[Chat]:
    """Get all chats involving the contact.
    
    Args:
        jid: The contact's JID to search for
        limit: Maximum number of chats to return (default 20)
        page: Page number for pagination (default 0)
    """
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            JOIN messages m ON c.jid = m.chat_jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY c.last_message_time DESC
            LIMIT ? OFFSET ?
        """, (jid, jid, limit, page * limit))
        
        chats = cursor.fetchall()
        
        result = []
        for chat_data in chats:
            chat = Chat(
                jid=chat_data[0],
                name=chat_data[1],
                last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
                last_message=chat_data[3],
                last_sender=chat_data[4],
                last_is_from_me=chat_data[5]
            )
            result.append(chat)
            
        return result
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return []
    finally:
        if 'conn' in locals():
            conn.close()


def get_last_interaction(jid: str) -> str:
    """Get most recent message involving the contact."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                m.timestamp,
                m.sender,
                c.name,
                m.content,
                m.is_from_me,
                c.jid,
                m.id,
                m.media_type
            FROM messages m
            JOIN chats c ON m.chat_jid = c.jid
            WHERE m.sender = ? OR c.jid = ?
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, (jid, jid))
        
        msg_data = cursor.fetchone()
        
        if not msg_data:
            return None
            
        message = Message(
            timestamp=datetime.fromisoformat(msg_data[0]),
            sender=msg_data[1],
            chat_name=msg_data[2],
            content=msg_data[3],
            is_from_me=msg_data[4],
            chat_jid=msg_data[5],
            id=msg_data[6],
            media_type=msg_data[7]
        )
        
        return format_message(message)
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_chat(chat_jid: str, include_last_message: bool = True) -> Optional[Chat]:
    """Get chat metadata by JID."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        query = """
            SELECT 
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
        """
        
        if include_last_message:
            query += """
                LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND c.last_message_time = m.timestamp
            """
            
        query += " WHERE c.jid = ?"
        
        cursor.execute(query, (chat_jid,))
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()


def get_direct_chat_by_contact(sender_phone_number: str) -> Optional[Chat]:
    """Get chat metadata by sender phone number."""
    try:
        conn = _connect_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                c.jid,
                c.name,
                c.last_message_time,
                m.content as last_message,
                m.sender as last_sender,
                m.is_from_me as last_is_from_me
            FROM chats c
            LEFT JOIN messages m ON c.jid = m.chat_jid 
                AND c.last_message_time = m.timestamp
            WHERE c.jid LIKE ? AND c.jid NOT LIKE '%@g.us'
            LIMIT 1
        """, (f"%{sender_phone_number}%",))
        
        chat_data = cursor.fetchone()
        
        if not chat_data:
            return None
            
        return Chat(
            jid=chat_data[0],
            name=chat_data[1],
            last_message_time=datetime.fromisoformat(chat_data[2]) if chat_data[2] else None,
            last_message=chat_data[3],
            last_sender=chat_data[4],
            last_is_from_me=chat_data[5]
        )
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if 'conn' in locals():
            conn.close()

def list_group_members(group_jid: str) -> Tuple[bool, Any]:
    """List a group's members (jid, phone, name, is_admin). The `phone` is the
    number to write as "@<phone>" in a message to mention that member."""
    try:
        if not group_jid or "@g.us" not in group_jid:
            return False, "A group JID (…@g.us) is required"
        url = f"{WHATSAPP_API_BASE_URL}/group-members"
        response = requests.get(url, params={"jid": group_jid}, timeout=BRIDGE_TIMEOUT)
        if response.status_code == 200:
            result = response.json()
            if result.get("success"):
                return True, result.get("members", [])
            return False, result.get("error", "Unknown error")
        return False, f"Error: HTTP {response.status_code} - {response.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def send_message(recipient: str, message: str, reply_to: Optional[str] = None,
                 reply_participant: Optional[str] = None,
                 reply_text: Optional[str] = None,
                 mentions: Optional[List[str]] = None) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"

        # Native reply: if we're quoting a message but weren't told what it said
        # / who sent it, look it up from the DB so WhatsApp shows the proper
        # quoted preview.
        if reply_to and (reply_text is None or not reply_participant):
            quoted = _lookup_message(reply_to, recipient)
            if quoted:
                reply_participant = reply_participant or quoted.get("sender")
                if reply_text is None:
                    reply_text = quoted.get("content") or ""

        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "message": message,
        }
        if reply_to:
            payload["reply_to"] = reply_to
            if reply_participant:
                payload["reply_participant"] = reply_participant
            payload["reply_text"] = reply_text or ""
        if mentions:
            payload["mentions"] = mentions

        response = requests.post(url, json=payload, timeout=BRIDGE_TIMEOUT)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_file(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload, timeout=BRIDGE_TIMEOUT)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def send_audio_message(recipient: str, media_path: str) -> Tuple[bool, str]:
    try:
        # Validate input
        if not recipient:
            return False, "Recipient must be provided"
        
        if not media_path:
            return False, "Media path must be provided"
        
        if not os.path.isfile(media_path):
            return False, f"Media file not found: {media_path}"

        if not media_path.endswith(".ogg"):
            try:
                media_path = audio.convert_to_opus_ogg_temp(media_path)
            except Exception as e:
                return False, f"Error converting file to opus ogg. You likely need to install ffmpeg: {str(e)}"
        
        url = f"{WHATSAPP_API_BASE_URL}/send"
        payload = {
            "recipient": recipient,
            "media_path": media_path
        }
        
        response = requests.post(url, json=payload, timeout=BRIDGE_TIMEOUT)
        
        # Check if the request was successful
        if response.status_code == 200:
            result = response.json()
            return result.get("success", False), result.get("message", "Unknown response")
        else:
            return False, f"Error: HTTP {response.status_code} - {response.text}"
            
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except json.JSONDecodeError:
        return False, f"Error parsing response: {response.text}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def download_media(message_id: str, chat_jid: str) -> Optional[str]:
    """Download media from a message and return the local file path.
    
    Args:
        message_id: The ID of the message containing the media
        chat_jid: The JID of the chat containing the message
    
    Returns:
        The local file path if download was successful, None otherwise
    """
    try:
        url = f"{WHATSAPP_API_BASE_URL}/download"
        payload = {
            "message_id": message_id,
            "chat_jid": chat_jid
        }
        
        response = requests.post(url, json=payload, timeout=BRIDGE_TIMEOUT)
        
        if response.status_code == 200:
            result = response.json()
            if result.get("success", False):
                path = result.get("path")
                print(f"Media downloaded successfully: {path}")
                return path
            else:
                print(f"Download failed: {result.get('message', 'Unknown error')}")
                return None
        else:
            print(f"Error: HTTP {response.status_code} - {response.text}")
            return None
            
    except requests.RequestException as e:
        print(f"Request error: {str(e)}")
        return None
    except json.JSONDecodeError:
        print(f"Error parsing response: {response.text}")
        return None
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return None


# ---------------------------------------------------------------------------
# New helpers / functions (native reply, direct DB read, reactions, read
# receipts, typing presence)
# ---------------------------------------------------------------------------

def _lookup_message(message_id: str, chat_jid: Optional[str] = None) -> Optional[dict]:
    """Return {'sender','content','chat_jid'} for a message id, or None."""
    conn = None
    try:
        conn = _connect_db()
        cur = conn.cursor()
        if chat_jid:
            cur.execute(
                "SELECT sender, content, chat_jid FROM messages WHERE id = ? AND chat_jid = ? LIMIT 1",
                (message_id, chat_jid),
            )
        else:
            cur.execute(
                "SELECT sender, content, chat_jid FROM messages WHERE id = ? LIMIT 1",
                (message_id,),
            )
        row = cur.fetchone()
        if row:
            return {"sender": row[0], "content": row[1], "chat_jid": row[2]}
        return None
    except sqlite3.Error as e:
        print(f"DB error in _lookup_message: {e}")
        return None
    finally:
        if conn:
            conn.close()


def query_database(query: str, params: Optional[list] = None, limit: int = 200) -> dict:
    """Run a READ-ONLY SQL query against the WhatsApp messages DB.

    Only SELECT / WITH / PRAGMA / EXPLAIN are allowed. Useful as a direct,
    reliable fallback when the higher-level tools get stuck. Tables: chats,
    messages.
    """
    q = (query or "").strip().rstrip(";")
    low = q.lstrip("(").lower()
    if not (low.startswith("select") or low.startswith("with")
            or low.startswith("pragma") or low.startswith("explain")):
        return {"success": False, "error": "Only read-only queries (SELECT/WITH/PRAGMA/EXPLAIN) are allowed."}
    padded = f" {low} "
    for kw in (" insert ", " update ", " delete ", " drop ", " alter ",
               " create ", " replace ", " attach ", " detach "):
        if kw in padded:
            return {"success": False, "error": "Write/DDL keywords are not allowed."}
    conn = None
    try:
        conn = _connect_db()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(q, tuple(params) if params else ())
        rows = cur.fetchmany(max(1, min(limit, 1000)))
        return {"success": True, "row_count": len(rows), "rows": [dict(r) for r in rows]}
    except sqlite3.Error as e:
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()


def send_reaction(chat_jid: str, message_id: str, emoji: str,
                  sender: Optional[str] = None) -> Tuple[bool, str]:
    """React to a message with an emoji. Empty emoji removes the reaction."""
    try:
        if not chat_jid or not message_id:
            return False, "chat_jid and message_id are required"
        if sender is None:
            q = _lookup_message(message_id, chat_jid)
            sender = q.get("sender") if q else None
        payload = {"recipient": chat_jid, "message_id": message_id, "emoji": emoji or ""}
        if sender:
            payload["sender"] = sender
        r = requests.post(f"{WHATSAPP_API_BASE_URL}/react", json=payload, timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            res = r.json()
            return res.get("success", False), res.get("message", "Unknown response")
        return False, f"Error: HTTP {r.status_code} - {r.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def mark_as_read(chat_jid: str, message_id: Optional[str] = None,
                 sender: Optional[str] = None) -> Tuple[bool, str]:
    """Mark a message (or the chat's latest incoming message) as read."""
    conn = None
    try:
        if not chat_jid:
            return False, "chat_jid is required"
        if message_id is None or sender is None:
            conn = _connect_db()
            cur = conn.cursor()
            if message_id:
                cur.execute("SELECT id, sender FROM messages WHERE id=? AND chat_jid=? LIMIT 1",
                            (message_id, chat_jid))
            else:
                cur.execute("SELECT id, sender FROM messages WHERE chat_jid=? AND is_from_me=0 "
                            "ORDER BY timestamp DESC LIMIT 1", (chat_jid,))
            row = cur.fetchone()
            if row:
                message_id = message_id or row[0]
                sender = sender or row[1]
        if not message_id:
            return False, "No message found to mark as read"
        payload = {"recipient": chat_jid, "message_id": message_id}
        if sender:
            payload["sender"] = sender
        r = requests.post(f"{WHATSAPP_API_BASE_URL}/markread", json=payload, timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            res = r.json()
            return res.get("success", False), res.get("message", "Unknown response")
        return False, f"Error: HTTP {r.status_code} - {r.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"
    finally:
        if conn:
            conn.close()


def send_presence(chat_jid: str, state: str = "composing") -> Tuple[bool, str]:
    """Show presence in a chat: 'composing' (typing), 'recording', or 'paused'."""
    try:
        if not chat_jid:
            return False, "chat_jid is required"
        if state not in ("composing", "recording", "paused"):
            return False, "state must be 'composing', 'recording' or 'paused'"
        r = requests.post(f"{WHATSAPP_API_BASE_URL}/presence",
                          json={"recipient": chat_jid, "state": state}, timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            res = r.json()
            return res.get("success", False), res.get("message", "Unknown response")
        return False, f"Error: HTTP {r.status_code} - {r.text}"
    except requests.RequestException as e:
        return False, f"Request error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


def subscribe_chat(chat_jid: str, target: str, kind: str = "cmux", include_own: bool = False) -> dict:
    """Register a webhook so new messages in chat_jid are pushed to `target`."""
    try:
        r = requests.post(f"{WHATSAPP_API_BASE_URL}/webhooks",
                          json={"chat_jid": chat_jid, "target": target,
                                "kind": kind, "include_own": include_own},
                          timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return {"success": False, "message": f"HTTP {r.status_code} - {r.text}"}
    except Exception as e:
        return {"success": False, "message": f"Request error: {str(e)}"}


def unsubscribe_chat(chat_jid: str, target: str, kind: str = "cmux") -> dict:
    """Remove a previously registered webhook subscription."""
    try:
        r = requests.delete(f"{WHATSAPP_API_BASE_URL}/webhooks",
                            json={"chat_jid": chat_jid, "target": target, "kind": kind},
                            timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return {"success": False, "message": f"HTTP {r.status_code} - {r.text}"}
    except Exception as e:
        return {"success": False, "message": f"Request error: {str(e)}"}


def list_subscriptions() -> dict:
    """List all active webhook subscriptions."""
    try:
        r = requests.get(f"{WHATSAPP_API_BASE_URL}/webhooks", timeout=BRIDGE_TIMEOUT)
        if r.status_code == 200:
            return r.json()
        return {"success": False, "message": f"HTTP {r.status_code} - {r.text}"}
    except Exception as e:
        return {"success": False, "message": f"Request error: {str(e)}"}
