import msal
import requests
from datetime import datetime
from src.EmailMetadata import EmailMetadata
# import pytz
import logging

logger = logging.getLogger("outlook-email.graph")


class GraphConnector:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, user_email: str):
        """
        Microsoft Graph API connector for reading emails.

        Args:
            tenant_id (str): Azure Directory (tenant) ID
            client_id (str): Application (client) ID
            client_secret (str): Client secret generated from Azure
            user_email (str): The mailbox to read (ex: ci@zapcg.com)
        """
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_email = user_email
        self.token = None

    # -----------------------------------------------------
    # AUTHENTICATION
    # -----------------------------------------------------
    def authenticate(self):
        logger.info("Authenticating with Microsoft Graph...")

        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )

        result = app.acquire_token_silent(
            scopes=["https://graph.microsoft.com/.default"],
            account=None
        )

        if not result:
            result = app.acquire_token_for_client(
                scopes=["https://graph.microsoft.com/.default"]
            )

        if "access_token" not in result:
            raise Exception(f"Failed to authenticate with Graph API: {result}")

        self.token = result["access_token"]
        logger.info("Graph authentication successful.")

    def headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    # -----------------------------------------------------
    # GET EMAILS
    # -----------------------------------------------------
    def get_emails(self, start_iso: str, end_iso: str):
        """
        Fetch emails from Microsoft Graph API for a date range with paging support.
        
        Args:
            start_iso: Start date in ISO format
            end_iso: End date in ISO format
            
        Returns:
            List of EmailMetadata objects
        """
        logger.info(f"Graph: Fetching emails from {start_iso} to {end_iso}")
        self.authenticate()

        # Ensure proper Graph-compatible datetime with timezone
        if not start_iso.endswith("Z"):
            start_iso = start_iso.replace("+00:00", "").replace("Z", "") + "Z"
        
        if not end_iso.endswith("Z"):
            end_iso = end_iso.replace("+00:00", "").replace("Z", "") + "Z"

        # Extended field selection including thread metadata
        select_fields = (
            "id,subject,from,receivedDateTime,body,toRecipients,"
            "conversationId,conversationIndex,internetMessageId,"
            "ccRecipients,replyTo,bodyPreview"
        )

        url = (
            f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages?"
            f"$filter=receivedDateTime ge {start_iso} and receivedDateTime le {end_iso}"
            f"&$select={select_fields}"
            f"&$top=50"
            f"&$orderby=receivedDateTime asc"
        )

        logger.info(f"Graph API URL: {url}")

        all_emails = []
        next_link = url

        # Handle paging
        while next_link:
            response = requests.get(next_link, headers=self.headers())
            if response.status_code != 200:
                logger.error(f"Graph API Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            messages = data.get("value", [])
            logger.info(f"Graph returned {len(messages)} messages in this page")

            for msg in messages:
                try:
                    received_dt = datetime.fromisoformat(
                        msg["receivedDateTime"].replace("Z", "+00:00")
                    )

                    sender = msg.get("from", {}).get("emailAddress", {})
                    sender_name = sender.get("name", "")
                    sender_email = sender.get("address", "")

                    to_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("toRecipients", [])
                    ]
                    to_field = ", ".join(to_list)

                    cc_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("ccRecipients", [])
                    ]
                    cc_field = ", ".join(cc_list) if cc_list else None

                    reply_to_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("replyTo", [])
                    ]
                    reply_to_field = ", ".join(reply_to_list) if reply_to_list else None

                    body_content = msg.get("body", {}).get("content", "")
                    body_preview = msg.get("bodyPreview", "")

                    email_meta = EmailMetadata(
                        AccountName=self.user_email,
                        Entry_ID=msg["id"],
                        Folder="Inbox",
                        Subject=msg.get("subject", ""),
                        SenderName=sender_name,
                        SenderEmailAddress=sender_email,
                        ReceivedTime=received_dt,
                        SentOn=received_dt,
                        To=to_field,
                        Body=body_content,
                        Attachments=[],
                        IsMarkedAsTask=False,
                        UnRead=False,
                        Categories="",
                        ConversationId=msg.get("conversationId"),
                        ConversationIndex=msg.get("conversationIndex"),
                        InternetMessageId=msg.get("internetMessageId"),
                        InReplyTo=msg.get("inReplyTo"),
                        CcRecipients=cc_field,
                        ReplyTo=reply_to_field,
                        BodyPreview=body_preview
                    )

                    all_emails.append(email_meta)

                except Exception as e:
                    logger.error(f"Error converting Graph email: {str(e)}")
                    continue

            # Check for next page
            next_link = data.get("@odata.nextLink")
            if next_link:
                logger.info(f"Fetching next page of results...")

        logger.info(f"Successfully converted {len(all_emails)} messages total.")
        return all_emails

    def sync_all_emails(self, delta_link: str = None):
        """
        Sync all emails using Graph delta query for full mailbox history.
        This is more efficient than date-range queries for initial sync.
        
        Args:
            delta_link: Previous delta link to continue from (None for initial sync)
            
        Returns:
            Tuple of (list of EmailMetadata objects, new delta_link)
        """
        logger.info("Graph: Starting delta sync for all emails")
        self.authenticate()

        select_fields = (
            "id,subject,from,receivedDateTime,body,toRecipients,"
            "conversationId,conversationIndex,internetMessageId,"
            "ccRecipients,replyTo,bodyPreview"
        )

        if delta_link:
            url = delta_link
            logger.info(f"Continuing delta sync from previous link")
        else:
            url = (
                f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/delta?"
                f"$select={select_fields}"
                f"&$top=50"
            )
            logger.info(f"Starting new delta sync")

        all_emails = []
        next_link = url
        new_delta_link = None

        while next_link:
            response = requests.get(next_link, headers=self.headers())
            if response.status_code != 200:
                logger.error(f"Graph API Error: {response.status_code} - {response.text}")
                break

            data = response.json()
            messages = data.get("value", [])
            logger.info(f"Delta sync returned {len(messages)} messages in this page")

            for msg in messages:
                try:
                    received_dt = datetime.fromisoformat(
                        msg["receivedDateTime"].replace("Z", "+00:00")
                    )

                    sender = msg.get("from", {}).get("emailAddress", {})
                    sender_name = sender.get("name", "")
                    sender_email = sender.get("address", "")

                    to_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("toRecipients", [])
                    ]
                    to_field = ", ".join(to_list)

                    cc_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("ccRecipients", [])
                    ]
                    cc_field = ", ".join(cc_list) if cc_list else None

                    reply_to_list = [
                        r["emailAddress"]["address"]
                        for r in msg.get("replyTo", [])
                    ]
                    reply_to_field = ", ".join(reply_to_list) if reply_to_list else None

                    body_content = msg.get("body", {}).get("content", "")
                    body_preview = msg.get("bodyPreview", "")

                    email_meta = EmailMetadata(
                        AccountName=self.user_email,
                        Entry_ID=msg["id"],
                        Folder="Inbox",
                        Subject=msg.get("subject", ""),
                        SenderName=sender_name,
                        SenderEmailAddress=sender_email,
                        ReceivedTime=received_dt,
                        SentOn=received_dt,
                        To=to_field,
                        Body=body_content,
                        Attachments=[],
                        IsMarkedAsTask=False,
                        UnRead=False,
                        Categories="",
                        ConversationId=msg.get("conversationId"),
                        ConversationIndex=msg.get("conversationIndex"),
                        InternetMessageId=msg.get("internetMessageId"),
                        InReplyTo=msg.get("inReplyTo"),
                        CcRecipients=cc_field,
                        ReplyTo=reply_to_field,
                        BodyPreview=body_preview
                    )

                    all_emails.append(email_meta)

                except Exception as e:
                    logger.error(f"Error converting Graph email: {str(e)}")
                    continue

            # Check for next page or delta link
            next_link = data.get("@odata.nextLink")
            if not next_link:
                # Delta sync provides a deltaLink at the end
                new_delta_link = data.get("@odata.deltaLink")
                if new_delta_link:
                    logger.info("Delta sync complete, received deltaLink for future syncs")
                break

        logger.info(f"Delta sync complete: {len(all_emails)} messages total")
        return all_emails, new_delta_link

    def get_message_attachments(self, message_id: str) -> list:
        """
        Get list of attachments for a message.
        Handles both fileAttachments and itemAttachments (embedded messages).

        Args:
            message_id: The message ID

        Returns:
            List of attachment dictionaries with id, name, contentType, size, attachmentType
        """
        try:
            self.authenticate()
            url = f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/{message_id}/attachments"

            response = requests.get(url, headers=self.headers())
            if response.status_code != 200:
                logger.error(f"Error getting attachments: {response.status_code} - {response.text}")
                return []

            data = response.json()
            attachments = data.get("value", [])

            attachment_list = []
            for att in attachments:
                attachment_type = att.get('@odata.type', '').split('.')[-1]  # Get type like 'fileAttachment' or 'itemAttachment'
                
                attachment_info = {
                    'id': att.get('id'),
                    'name': att.get('name'),
                    'contentType': att.get('contentType'),
                    'size': att.get('size', 0),
                    'isInline': att.get('isInline', False),
                    'attachmentType': attachment_type
                }
                
                # For itemAttachments (embedded messages), get the item info
                if attachment_type == 'itemAttachment':
                    item = att.get('item', {})
                    attachment_info['itemId'] = item.get('id')
                    attachment_info['itemType'] = item.get('@odata.type', '').split('.')[-1]  # Usually 'message'
                    # If contentType is None, set it based on item type
                    if not attachment_info['contentType']:
                        if attachment_info['itemType'] == 'message':
                            attachment_info['contentType'] = 'application/vnd.ms-outlook'
                
                attachment_list.append(attachment_info)

            logger.info(f"Found {len(attachment_list)} attachments for message {message_id}")
            return attachment_list

        except Exception as e:
            logger.error(f"Error getting message attachments: {str(e)}", exc_info=True)
            return []

    def download_attachment(self, message_id: str, attachment_id: str, attachment_type: str = None) -> bytes:
        """
        Download attachment binary content.
        For itemAttachments (embedded messages), extracts the message content and converts to .msg-like format.

        Args:
            message_id: The message ID
            attachment_id: The attachment ID
            attachment_type: Type of attachment ('fileAttachment' or 'itemAttachment')

        Returns:
            Attachment bytes or None if error
        """
        try:
            self.authenticate()
            
            # For itemAttachments, we need to get the embedded message and convert it
            if attachment_type == 'itemAttachment':
                # Get the attachment to get its name (which is usually the subject)
                url = f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/{message_id}/attachments/{attachment_id}"
                response = requests.get(url, headers=self.headers())
                
                if response.status_code != 200:
                    logger.error(f"Error getting itemAttachment: {response.status_code} - {response.text}")
                    return None
                
                attachment_data = response.json()
                attachment_name = attachment_data.get('name', '')
                
                # For itemAttachments, the name is typically the subject of the embedded message
                # Try to find the message in the mailbox by subject (using contains for better matching)
                # Escape single quotes in subject for OData filter
                escaped_subject = attachment_name.replace("'", "''")
                
                # Try exact match first
                search_url = (
                    f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages?"
                    f"$filter=subject eq '{escaped_subject}'"
                    f"&$top=1"
                    f"&$orderby=receivedDateTime desc"
                )
                
                search_response = requests.get(search_url, headers=self.headers())
                item = None
                
                if search_response.status_code == 200:
                    search_data = search_response.json()
                    messages = search_data.get('value', [])
                    if messages:
                        # Found matching message, get full details
                        found_msg_id = messages[0].get('id')
                        msg_url = f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/{found_msg_id}?$select=subject,from,receivedDateTime,body,bodyPreview,toRecipients,ccRecipients"
                        msg_response = requests.get(msg_url, headers=self.headers())
                        if msg_response.status_code == 200:
                            item = msg_response.json()
                            logger.info(f"Found embedded message by exact subject match: {attachment_name}")
                
                # If exact match failed, try contains (for forwarded emails with "Fw:" prefix variations)
                if not item:
                    # Remove common prefixes and try again
                    clean_subject = attachment_name
                    for prefix in ['FW:', 'Fw:', 'Re:', 'RE:']:
                        if clean_subject.startswith(prefix):
                            clean_subject = clean_subject[len(prefix):].strip()
                            break
                    
                    escaped_clean = clean_subject.replace("'", "''")
                    search_url = (
                        f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages?"
                        f"$filter=contains(subject, '{escaped_clean[:50]}')"  # Limit length for OData
                        f"&$top=5"
                        f"&$orderby=receivedDateTime desc"
                    )
                    
                    search_response = requests.get(search_url, headers=self.headers())
                    if search_response.status_code == 200:
                        search_data = search_response.json()
                        messages = search_data.get('value', [])
                        # Find the best match
                        for msg in messages:
                            msg_subject = msg.get('subject', '')
                            # Check if subjects are similar (ignoring case and prefixes)
                            if (clean_subject.lower() in msg_subject.lower() or 
                                msg_subject.lower() in clean_subject.lower()):
                                found_msg_id = msg.get('id')
                                msg_url = f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/{found_msg_id}?$select=subject,from,receivedDateTime,body,bodyPreview,toRecipients,ccRecipients"
                                msg_response = requests.get(msg_url, headers=self.headers())
                                if msg_response.status_code == 200:
                                    item = msg_response.json()
                                    logger.info(f"Found embedded message by partial subject match: {attachment_name}")
                                    break
                
                if not item:
                    # If we can't find the message, create a placeholder with the attachment name
                    logger.warning(f"Could not find embedded message for itemAttachment: {attachment_name}")
                    # Return a formatted text with just the subject
                    email_text = f"Subject: {attachment_name}\n\n[Embedded email message - full content not available via Graph API]"
                    return email_text.encode('utf-8')
                
                # Extract message content and format as text (simulating .msg content)
                subject = item.get('subject', '') if item else ''
                sender = item.get('from', {}).get('emailAddress', {}) if item and item.get('from') else {}
                sender_name = sender.get('name', '') if sender else ''
                sender_email = sender.get('address', '') if sender else ''
                sender_str = f"{sender_name} <{sender_email}>" if sender_name or sender_email else ''
                
                # Get body content (prefer HTML, fallback to plain text)
                body_content = ''
                if item:
                    body_obj = item.get('body', {})
                    if body_obj:
                        body_content = body_obj.get('content', '')
                    if not body_content:
                        body_content = item.get('bodyPreview', '')
                
                # Strip HTML tags if present
                if body_content and '<' in body_content:
                    try:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(body_content, 'html.parser')
                        body_content = soup.get_text(separator='\n', strip=True)
                    except Exception:
                        # If BeautifulSoup fails, use body as-is
                        pass
                
                received_time = item.get('receivedDateTime', '') if item else ''
                
                # Get recipients
                to_list = ''
                cc_list = ''
                if item:
                    to_recipients = item.get('toRecipients', [])
                    to_list = ', '.join([r.get('emailAddress', {}).get('address', '') for r in to_recipients if r.get('emailAddress', {}).get('address')])
                    
                    cc_recipients = item.get('ccRecipients', [])
                    cc_list = ', '.join([r.get('emailAddress', {}).get('address', '') for r in cc_recipients if r.get('emailAddress', {}).get('address')])
                
                # Format as email text (similar to what extract-msg would produce)
                email_text = f"""Subject: {subject}
From: {sender_str}
To: {to_list}
CC: {cc_list}
Date: {received_time}

{body_content}
"""
                
                logger.info(f"Extracted embedded message: {subject[:50]}... ({len(email_text)} chars)")
                return email_text.encode('utf-8')
            else:
                # For fileAttachments, download the binary content
                url = f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages/{message_id}/attachments/{attachment_id}/$value"
                response = requests.get(url, headers=self.headers())
                if response.status_code != 200:
                    logger.error(f"Error downloading attachment: {response.status_code} - {response.text}")
                    return None

                logger.info(f"Downloaded attachment {attachment_id} ({len(response.content)} bytes)")
                return response.content

        except Exception as e:
            logger.error(f"Error downloading attachment: {str(e)}", exc_info=True)
            return None
