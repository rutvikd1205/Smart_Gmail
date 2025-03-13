import logging
import os
import imaplib
from email import message
from email.message import Message
from email import message_from_bytes, utils
import yaml
from datetime import datetime
from typing import Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# IMAP Configuration
IMAP_URL = 'imap.gmail.com'
CREDENTIALS_YAML = os.getenv('CREDENTIALS_YAML', 'credentials.yml')

class GmailClient:
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.imap = None
        self.password = None
        self._load_credentials()
        self._authenticate_imap()

    def _load_credentials(self):
        """Load credentials from YAML file"""
        try:
            if not os.path.exists(CREDENTIALS_YAML):
                raise FileNotFoundError(
                    f"credentials.yml not found. Please create it with your Gmail credentials."
                )
            
            with open(CREDENTIALS_YAML) as f:
                content = f.read()
                credentials = yaml.load(content, Loader=yaml.FullLoader)
                
            if not credentials or 'user' not in credentials or 'password' not in credentials:
                raise ValueError("credentials.yml must contain 'user' and 'password' fields")
                
            if credentials['user'] != self.user_id:
                raise ValueError(f"User ID mismatch. Expected {self.user_id}, got {credentials['user']}")
                
            self.password = credentials['password']
            logger.info("Successfully loaded credentials from YAML file")
            
        except Exception as e:
            logger.error(f"Error loading credentials: {str(e)}")
            raise

    def _authenticate_imap(self):
        """Authenticate with Gmail using IMAP"""
        try:
            if not self.password:
                raise ValueError("Password not loaded from credentials file")

            # Connect to IMAP server
            self.imap = imaplib.IMAP4_SSL(IMAP_URL)
            self.imap.login(self.user_id, self.password)
            logger.info(f"Successfully authenticated user {self.user_id} with IMAP")
        except Exception as e:
            logger.error(f"IMAP authentication error for user {self.user_id}: {str(e)}")
            raise

    def _get_email_body_imap(self, msg: Message) -> str:
        """Extract email body using IMAP"""
        try:
            if msg.is_multipart():
                # Try to get text/plain part first
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        try:
                            return part.get_payload(decode=True).decode('utf-8', errors='replace')
                        except Exception as e:
                            logger.error(f"Error decoding text/plain part: {str(e)}")
                            continue
                
                # If no text/plain, try text/html
                for part in msg.walk():
                    if part.get_content_type() == 'text/html':
                        try:
                            return "Email contains HTML content"
                        except Exception as e:
                            logger.error(f"Error decoding text/html part: {str(e)}")
                            continue
                
                # If still no content, get first part
                return self._get_email_body_imap(msg.get_payload(0))
            else:
                try:
                    return msg.get_payload(decode=True).decode('utf-8', errors='replace')
                except Exception as e:
                    logger.error(f"Error decoding single part email: {str(e)}")
                    return "Error decoding email body"
        except Exception as e:
            logger.error(f"Error extracting email body via IMAP: {str(e)}")
            return "Error extracting email body"

    def fetch_emails(
        self,
        max_results: int = None,
        after_date: Optional[datetime] = None,
        query: str = ''
    ) -> List[Dict]:
        """Fetch emails using IMAP"""
        try:
            # Select inbox
            self.imap.select('INBOX')
            
            # Build search criteria
            search_criteria = []
            if after_date:
                date_str = after_date.strftime('%d-%b-%Y')
                search_criteria.append(f'SINCE "{date_str}"')
                logger.info(f"Searching for emails since {date_str}")
            if query:
                search_criteria.append(query)
            
            # Search for emails
            search_str = ' '.join(search_criteria)
            _, message_numbers = self.imap.search(None, search_str)
            
            emails = []
            message_list = message_numbers[0].split()
            
            # If max_results is specified, limit the results
            if max_results and len(message_list) > max_results:
                message_list = message_list[-max_results:]
            
            total_messages = len(message_list)
            logger.info(f"Found {total_messages} total emails to process")
            
            filtered_count = 0
            for i, num in enumerate(message_list, 1):
                try:
                    _, msg_data = self.imap.fetch(num, '(RFC822)')
                    email_body = msg_data[0][1]
                    msg = message_from_bytes(email_body)
                    
                    # Extract headers
                    subject = msg.get('subject', '')
                    sender = msg.get('from', '')
                    date_str = msg.get('date', '')
                    
                    # Parse the date string
                    try:
                        # Try to parse the date string
                        date_tuple = utils.parsedate_tz(date_str)
                        if date_tuple:
                            date = datetime.fromtimestamp(utils.mktime_tz(date_tuple))
                        else:
                            date = datetime.now()
                    except Exception as e:
                        logger.error(f"Error parsing date {date_str}: {str(e)}")
                        date = datetime.now()
                    
                    # Skip if email is older than after_date
                    if after_date and date < after_date:
                        filtered_count += 1
                        logger.debug(f"Skipping email from {date} (older than {after_date})")
                        continue
                    
                    # Get email body
                    body = self._get_email_body_imap(msg)
                    
                    emails.append({
                        'message_id': num.decode('utf-8'),
                        'subject': subject,
                        'sender': sender,
                        'received_at': date.strftime('%Y-%m-%d %H:%M:%S'),
                        'body': body,
                        'processed': False
                    })
                    
                    # Log progress every 100 emails
                    if i % 100 == 0:
                        logger.info(f"Processed {i}/{total_messages} emails ({(i/total_messages)*100:.1f}%)")
                    
                except Exception as e:
                    logger.error(f"Error processing IMAP message {num}: {str(e)}")
                    continue
            
            logger.info(f"Successfully fetched {len(emails)} new emails via IMAP for user {self.user_id}")
            if emails:
                logger.info(f"Date range: {emails[-1]['received_at']} to {emails[0]['received_at']}")
                logger.info(f"Filtered {filtered_count} emails older than {after_date}")
                logger.info(f"Remaining {len(emails)} emails are newer than {after_date}")
            return emails
            
        except Exception as e:
            logger.error(f"Error fetching emails via IMAP for user {self.user_id}: {str(e)}")
            raise

    def __del__(self):
        """Cleanup IMAP connection if it exists"""
        if self.imap:
            try:
                self.imap.close()
                self.imap.logout()
            except:
                pass 