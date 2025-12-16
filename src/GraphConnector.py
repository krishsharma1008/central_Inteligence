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
        logger.info(f"Graph: Fetching emails from {start_iso} to {end_iso}")
        self.authenticate()

        # # Ensure UTC format
        # if "T" not in start_iso:
        #     start_iso += "T00:00:00Z"
        # if "T" not in end_iso:
        #     end_iso += "T23:59:59Z"
        # Ensure proper Graph-compatible datetime with timezone
        if not start_iso.endswith("Z"):
            start_iso = start_iso.replace("+00:00", "").replace("Z", "") + "Z"
        
        if not end_iso.endswith("Z"):
            end_iso = end_iso.replace("+00:00", "").replace("Z", "") + "Z"

        url = (
            f"https://graph.microsoft.com/v1.0/users/{self.user_email}/messages?"
            f"$filter=receivedDateTime ge {start_iso} and receivedDateTime le {end_iso}"
            f"&$select=id,subject,from,receivedDateTime,body,toRecipients"
            f"&$top=50"
        )

        logger.info(f"Graph API URL: {url}")

        response = requests.get(url, headers=self.headers())
        if response.status_code != 200:
            logger.error(f"Graph API Error: {response.status_code} - {response.text}")
            return []

        data = response.json()
        messages = data.get("value", [])

        logger.info(f"Graph returned {len(messages)} messages")

        emails = []
        for msg in messages:
            try:
                received_dt = datetime.fromisoformat(
                    msg["receivedDateTime"].replace("Z", "+00:00")
                )

                sender = msg["from"]["emailAddress"]

                to_list = [
                    r["emailAddress"]["address"]
                    for r in msg.get("toRecipients", [])
                ]
                to_field = ", ".join(to_list)

                body_content = msg.get("body", {}).get("content", "")

                email_meta = EmailMetadata(
                    AccountName=self.user_email,
                    Entry_ID=msg["id"],
                    Folder="Inbox",
                    Subject=msg.get("subject", ""),
                    SenderName=sender.get("name", ""),
                    SenderEmailAddress=sender.get("address", ""),
                    ReceivedTime=received_dt,
                    SentOn=received_dt,
                    To=to_field,
                    Body=body_content,
                    Attachments=[],
                    IsMarkedAsTask=False,
                    UnRead=False,
                    Categories=""
                )

                emails.append(email_meta)

            except Exception as e:
                logger.error(f"Error converting Graph email: {str(e)}")
                continue

        logger.info(f"Successfully converted {len(emails)} messages.")
        return emails
