import logging
import os
import time
from typing import List, Dict

from db_client import DatabaseClient
from gmail_client import GmailClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get configuration from environment variables
MAX_EMAILS_PER_FETCH = int(os.getenv('MAX_EMAILS_PER_FETCH', '100'))
POLLING_INTERVAL = int(os.getenv('POLLING_INTERVAL', '300'))  # 5 minutes default

class EmailProcessor:
    def __init__(self):
        self.gmail_client = GmailClient()
        self.db_client = DatabaseClient()

    def process_emails(self):
        """Main processing loop"""
        try:
            # Fetch emails from Gmail
            logger.info("Fetching emails from Gmail...")
            emails = self.gmail_client.fetch_emails(
                max_results=MAX_EMAILS_PER_FETCH
            )
            
            if not emails:
                logger.info("No new emails found.")
                return

            logger.info(f"Found {len(emails)} emails")

            # Store emails in database
            stored_count = self.db_client.store_emails(emails)
            logger.info(f"Stored {stored_count} new emails in database")

        except Exception as e:
            logger.error(f"Error in process_emails: {str(e)}")

def main():
    processor = EmailProcessor()
    
    logger.info("Starting email processor...")
    
    while True:
        try:
            processor.process_emails()
            logger.info(f"Sleeping for {POLLING_INTERVAL} seconds...")
            time.sleep(POLLING_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
            
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    main() 