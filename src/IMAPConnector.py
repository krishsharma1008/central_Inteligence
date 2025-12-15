import imaplib
import email
from email.header import decode_header
from datetime import datetime
import pytz
from typing import List, Optional
import logging
from EmailMetadata import EmailMetadata
import re

# Configure logging
logger = logging.getLogger('outlook-email.imap')

class IMAPConnector:
    def __init__(self, email_address: str, password: str, imap_server: str = "outlook.office365.com", imap_port: int = 993):
        """
        Initialize the IMAP connector.
        
        Args:
            email_address (str): Email address for authentication
            password (str): App password for IMAP access
            imap_server (str): IMAP server address
            imap_port (int): IMAP port (default: 993 for SSL)
        """
        self.email_address = email_address
        self.password = password
        self.imap_server = imap_server
        self.imap_port = imap_port
        self.mail = None
        
    def connect(self, max_retries: int = 3) -> bool:
        """
        Connect to the IMAP server.
        
        Args:
            max_retries (int): Maximum number of connection attempts
            
        Returns:
            bool: True if connection successful
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Connecting to IMAP server {self.imap_server}:{self.imap_port}")
                self.mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
                self.mail.login(self.email_address, self.password)
                logger.info("Successfully connected to IMAP server")
                return True
            except Exception as e:
                logger.error(f"Error connecting to IMAP server (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt == max_retries - 1:
                    raise
                import time
                time.sleep(2)
        return False
    
    def disconnect(self) -> None:
        """Disconnect from the IMAP server."""
        try:
            if self.mail:
                self.mail.logout()
                logger.info("Disconnected from IMAP server")
        except Exception as e:
            logger.error(f"Error disconnecting from IMAP server: {str(e)}")
    
    @staticmethod
    def decode_mime_header(header_value: str) -> str:
        """Decode MIME-encoded email header."""
        if not header_value:
            return ""
        
        decoded_parts = []
        for part, encoding in decode_header(header_value):
            if isinstance(part, bytes):
                try:
                    decoded_parts.append(part.decode(encoding or 'utf-8', errors='ignore'))
                except Exception:
                    decoded_parts.append(part.decode('utf-8', errors='ignore'))
            else:
                decoded_parts.append(str(part))
        return ' '.join(decoded_parts)
    
    @staticmethod
    def clean_email_body(body: str) -> str:
        """Clean email body by removing problematic content."""
        if not body:
            return ""
        
        body = str(body)
        # Remove control characters
        body = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', body)
        # Normalize line endings
        body = re.sub(r'\r\n|\r|\n', ' ', body)
        # Collapse whitespace
        body = re.sub(r'\s+', ' ', body)
        
        return body.strip()
    
    def get_email_body(self, msg: email.message.Message) -> str:
        """Extract email body from message."""
        body = ""
        
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition"))
                
                # Skip attachments
                if "attachment" in content_disposition:
                    continue
                
                # Get text/plain or text/html
                if content_type == "text/plain":
                    try:
                        body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        break
                    except Exception:
                        continue
                elif content_type == "text/html" and not body:
                    try:
                        body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                    except Exception:
                        continue
        else:
            try:
                body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
            except Exception:
                body = str(msg.get_payload())
        
        return self.clean_email_body(body)
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse email date string to datetime object."""
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(date_str)
            # Convert to UTC
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            else:
                dt = dt.astimezone(pytz.UTC)
            return dt
        except Exception as e:
            logger.error(f"Error parsing date '{date_str}': {str(e)}")
            return None
    
    def get_emails_within_date_range(
        self,
        folder_names: List[str],
        start_date: str,
        end_date: str,
        mailboxes: Optional[List] = None
    ) -> List[EmailMetadata]:
        """
        Retrieve emails from INBOX within the specified date range.
        
        Args:
            folder_names (List[str]): Folder names (only "Inbox" is supported)
            start_date (str): Start date in ISO format
            end_date (str): End date in ISO format
            mailboxes (Optional[List]): Ignored for IMAP (uses configured email)
            
        Returns:
            List[EmailMetadata]: List of email metadata objects
        """
        email_data = []
        
        try:
            # Connect if not already connected
            if not self.mail:
                self.connect()
            
            # Select INBOX
            self.mail.select("INBOX")
            logger.info("Selected INBOX folder")
            
            # Convert dates for filtering
            start_datetime = datetime.fromisoformat(start_date)
            end_datetime = datetime.fromisoformat(end_date)
            
            # Convert to UTC
            local_tz = pytz.UTC
            start_utc = local_tz.localize(start_datetime.replace(hour=0, minute=0, second=0))
            end_utc = local_tz.localize(end_datetime.replace(hour=23, minute=59, second=59))
            
            # Format dates for IMAP search (format: DD-Mon-YYYY)
            start_search = start_datetime.strftime("%d-%b-%Y")
            end_search = end_datetime.strftime("%d-%b-%Y")
            
            # Search for emails in date range
            search_criteria = f'(SINCE {start_search} BEFORE {end_search})'
            logger.info(f"Searching emails with criteria: {search_criteria}")
            
            status, messages = self.mail.search(None, 'SINCE', start_search)
            
            if status != "OK":
                logger.error("Failed to search emails")
                return email_data
            
            email_ids = messages[0].split()
            logger.info(f"Found {len(email_ids)} emails in INBOX")
            
            for email_id in email_ids:
                try:
                    # Fetch email
                    status, msg_data = self.mail.fetch(email_id, "(RFC822)")
                    
                    if status != "OK":
                        continue
                    
                    # Parse email
                    for response_part in msg_data:
                        if isinstance(response_part, tuple):
                            msg = email.message_from_bytes(response_part[1])
                            
                            # Extract email metadata
                            subject = self.decode_mime_header(msg.get("Subject", ""))
                            from_header = self.decode_mime_header(msg.get("From", ""))
                            to_header = self.decode_mime_header(msg.get("To", ""))
                            date_header = msg.get("Date", "")
                            
                            # Parse date
                            received_time = self.parse_date(date_header)
                            if not received_time:
                                continue
                            
                            # Check if email is within date range
                            if not (start_utc <= received_time <= end_utc):
                                continue
                            
                            # Extract sender email from "From" header
                            sender_email = from_header
                            sender_name = from_header
                            if '<' in from_header and '>' in from_header:
                                sender_name = from_header.split('<')[0].strip()
                                sender_email = from_header.split('<')[1].split('>')[0].strip()
                            
                            # Get email body
                            body = self.get_email_body(msg)
                            
                            # Generate unique ID (use Message-ID if available)
                            message_id = msg.get("Message-ID", f"{email_id.decode()}_{date_header}")
                            
                            # Create EmailMetadata object
                            email_metadata = EmailMetadata(
                                AccountName=self.email_address,
                                Entry_ID=message_id,
                                Folder="Inbox",
                                Subject=subject,
                                SenderName=sender_name,
                                SenderEmailAddress=sender_email,
                                ReceivedTime=received_time,
                                SentOn=received_time,  # Use received time as sent time
                                To=to_header,
                                Body=body,
                                Attachments=[],  # Attachments not extracted for now
                                IsMarkedAsTask=False,
                                UnRead=False,
                                Categories=""
                            )
                            
                            email_data.append(email_metadata)
                            
                except Exception as e:
                    logger.error(f"Error processing email {email_id}: {str(e)}")
                    continue
            
            logger.info(f"Successfully retrieved {len(email_data)} emails")
            
        except Exception as e:
            logger.error(f"Error retrieving emails: {str(e)}")
            raise
        
        return email_data
    
    def get_mailboxes(self) -> List:
        """Return a list with the configured email address (for compatibility)."""
        class Mailbox:
            def __init__(self, display_name: str):
                self.DisplayName = display_name
        
        return [Mailbox(self.email_address)]
    
    def get_mailbox(self, mailbox_name: str):
        """Return mailbox if name matches (for compatibility)."""
        if mailbox_name.lower() == self.email_address.lower():
            class Mailbox:
                def __init__(self, display_name: str):
                    self.DisplayName = display_name
            return Mailbox(self.email_address)
        return None
    
    def __del__(self):
        """Destructor to ensure connection is closed."""
        self.disconnect()

