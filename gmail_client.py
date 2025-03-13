import base64
import json
import logging
import os
import pickle
from datetime import datetime
from email.mime.text import MIMEText
from typing import Dict, List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Gmail API Configuration
GMAIL_CREDENTIALS_FILE = os.getenv('GMAIL_CREDENTIALS_FILE', 'credentials.json')
GMAIL_TOKEN_FILE = os.getenv('GMAIL_TOKEN_FILE', 'token.pickle')
GMAIL_SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly'
]

class GmailClient:
    def __init__(self):
        self.service = self._authenticate()

    def _authenticate(self):
        """Authenticate with Gmail API using OAuth 2.0"""
        creds = None
        
        # Check if token.pickle exists
        if os.path.exists(GMAIL_TOKEN_FILE):
            with open(GMAIL_TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        
        # If credentials are not valid or don't exist, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not os.path.exists(GMAIL_CREDENTIALS_FILE):
                    raise FileNotFoundError(
                        f"credentials.json not found. Please download it from Google Cloud Console "
                        f"and place it in {os.path.abspath(GMAIL_CREDENTIALS_FILE)}"
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    GMAIL_CREDENTIALS_FILE, GMAIL_SCOPES)
                creds = flow.run_local_server(port=8000)
            
            # Save credentials for future use
            with open(GMAIL_TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
        
        return build('gmail', 'v1', credentials=creds)

    def fetch_emails(self, query: str = '', max_results: int = 100) -> List[Dict]:
        """Fetch emails from Gmail based on query"""
        try:
            results = self.service.users().messages().list(
                userId='me',
                q=query,
                maxResults=max_results
            ).execute()

            messages = results.get('messages', [])
            emails = []

            for message in messages:
                try:
                    email_data = self.service.users().messages().get(
                        userId='me',
                        id=message['id'],
                        format='full'
                    ).execute()

                    parsed_email = self._parse_email(email_data)
                    if parsed_email:
                        emails.append(parsed_email)
                except HttpError as e:
                    logger.error(f"Error fetching email {message['id']}: {str(e)}")
                    continue

            return emails

        except HttpError as e:
            logger.error(f"Error fetching emails: {str(e)}")
            return []

    def _parse_email(self, email_data: Dict) -> Optional[Dict]:
        """Parse email data from Gmail API response"""
        try:
            headers = {header['name']: header['value'] 
                      for header in email_data['payload']['headers']}

            body = ''
            if 'parts' in email_data['payload']:
                for part in email_data['payload']['parts']:
                    if part['mimeType'] == 'text/plain':
                        try:
                            body_data = part['body'].get('data', '')
                            if body_data:
                                decoded_bytes = base64.urlsafe_b64decode(body_data)
                                body = decoded_bytes.decode('utf-8', errors='replace')
                                break
                        except Exception as e:
                            logger.error(f"Error decoding email part: {str(e)}")
                            continue
            elif 'body' in email_data['payload']:
                try:
                    body_data = email_data['payload']['body'].get('data', '')
                    if body_data:
                        decoded_bytes = base64.urlsafe_b64decode(body_data)
                        body = decoded_bytes.decode('utf-8', errors='replace')
                except Exception as e:
                    logger.error(f"Error decoding email body: {str(e)}")

            return {
                'message_id': headers.get('Message-ID', ''),
                'subject': headers.get('Subject', ''),
                'sender': headers.get('From', ''),
                'received_at': headers.get('Date', ''),
                'body': body,
                'raw_email': email_data
            }

        except Exception as e:
            logger.error(f"Error parsing email: {str(e)}")
            return None 