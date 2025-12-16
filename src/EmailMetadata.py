from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
import re
import json
def validate_json(text: str, field_name: str = "") -> bool:
    """Test if a string can be properly encoded as JSON."""
    try:
        json.dumps(text)
        return True
    except (TypeError, ValueError):
        return False

def sanitize_text(text: str | None) -> str:
    """Sanitize text for JSON encoding."""
    if text is None:
        return ""
        
    # Convert to string if not already
    text = str(text)
    
    # Remove control characters and normalize
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', text)  # Remove control characters
    text = re.sub(r'\r\n|\r|\n', ' ', text)  # Normalize line endings to spaces
    text = re.sub(r'\s+', ' ', text)  # Collapse whitespace
    
    # Escape special characters
    text = text.replace('\\', '\\\\')
    text = text.replace('"', '\\"')
    text = text.replace('\t', ' ')
    
    return text.strip()

@dataclass
class EmailMetadata:
    AccountName: str
    Entry_ID: str
    Folder: str
    Subject: str
    SenderName: str
    SenderEmailAddress: str
    ReceivedTime: datetime
    SentOn: Optional[datetime]
    To: str
    Body: str
    Attachments: List[str]
    IsMarkedAsTask: bool
    UnRead: bool
    Categories: str
    GeneratedCategories: Optional[List[str]] = field(default_factory=list)
    embedding: Optional[List[float]] = field(default_factory=list)
    ConversationId: Optional[str] = None
    ConversationIndex: Optional[str] = None
    InternetMessageId: Optional[str] = None
    InReplyTo: Optional[str] = None
    CcRecipients: Optional[str] = None
    ReplyTo: Optional[str] = None
    BodyPreview: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert email metadata to a dictionary with validation."""
        try:
            # Create initial dictionary with validation
            raw_data = {}
            
            # Required fields
            required_fields = ['AccountName', 'Entry_ID', 'Folder', 'Subject', 'ReceivedTime']
            for field in required_fields:
                value = getattr(self, field, None)
                if value is None:
                    raise ValueError(f"Missing required field: {field}")
                if field in ['ReceivedTime', 'SentOn']:
                    raw_data[field] = value.isoformat() if value else None
                else:
                    raw_data[field] = sanitize_text(str(value))
            
            # Optional fields
            raw_data.update({
                "SenderName": sanitize_text(getattr(self, 'SenderName', '')),
                "SenderEmailAddress": sanitize_text(getattr(self, 'SenderEmailAddress', '')),
                "SentOn": self.SentOn.isoformat() if getattr(self, 'SentOn', None) else None,
                "To": sanitize_text(getattr(self, 'To', '')),
                "Body": sanitize_text(getattr(self, 'Body', '')),
                "Attachments": ', '.join(sanitize_text(att) for att in getattr(self, 'Attachments', [])),
                "IsMarkedAsTask": bool(getattr(self, 'IsMarkedAsTask', False)),
                "UnRead": bool(getattr(self, 'UnRead', False)),
                "Categories": sanitize_text(getattr(self, 'Categories', '')),
                "GeneratedCategories": ', '.join(sanitize_text(cat) for cat in (getattr(self, 'GeneratedCategories', []) or [])),
                "embedding": self.embedding if isinstance(getattr(self, 'embedding', None), list) else [],
                "ConversationId": sanitize_text(getattr(self, 'ConversationId', '') or ''),
                "ConversationIndex": sanitize_text(getattr(self, 'ConversationIndex', '') or ''),
                "InternetMessageId": sanitize_text(getattr(self, 'InternetMessageId', '') or ''),
                "InReplyTo": sanitize_text(getattr(self, 'InReplyTo', '') or ''),
                "CcRecipients": sanitize_text(getattr(self, 'CcRecipients', '') or ''),
                "ReplyTo": sanitize_text(getattr(self, 'ReplyTo', '') or ''),
                "BodyPreview": sanitize_text(getattr(self, 'BodyPreview', '') or '')
            })
            
            # Sanitize the data
            data = {
                "AccountName": sanitize_text(raw_data["AccountName"]),
                "Entry_ID": sanitize_text(raw_data["Entry_ID"]),
                "Folder": sanitize_text(raw_data["Folder"]),
                "Subject": sanitize_text(raw_data["Subject"]),
                "SenderName": sanitize_text(raw_data["SenderName"]),
                "SenderEmailAddress": sanitize_text(raw_data["SenderEmailAddress"]),
                "ReceivedTime": raw_data["ReceivedTime"],
                "SentOn": raw_data["SentOn"],
                "To": sanitize_text(raw_data["To"]),
                "Body": sanitize_text(raw_data["Body"]),
                "Attachments": raw_data["Attachments"],
                "IsMarkedAsTask": bool(raw_data["IsMarkedAsTask"]),
                "UnRead": bool(raw_data["UnRead"]),
                "Categories": sanitize_text(raw_data["Categories"]),
                "GeneratedCategories": raw_data["GeneratedCategories"],
                "embedding": raw_data["embedding"] if isinstance(raw_data["embedding"], list) else [],
                "ConversationId": raw_data.get("ConversationId", ""),
                "ConversationIndex": raw_data.get("ConversationIndex", ""),
                "InternetMessageId": raw_data.get("InternetMessageId", ""),
                "InReplyTo": raw_data.get("InReplyTo", ""),
                "CcRecipients": raw_data.get("CcRecipients", ""),
                "ReplyTo": raw_data.get("ReplyTo", ""),
                "BodyPreview": raw_data.get("BodyPreview", "")
            }
            
            # Validate each field can be properly encoded as JSON
            for key, value in data.items():
                if not validate_json(value, key):
                    raise ValueError(f"Field {key} contains invalid JSON data")
            
            # Validate the entire object can be encoded as JSON
            try:
                json.dumps(data)
                return data
            except (TypeError, ValueError) as e:
                raise ValueError(f"Email metadata cannot be encoded as JSON: {str(e)}")
                
        except Exception as e:
            raise
