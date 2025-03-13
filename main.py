import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict
import yaml

from db_client import DatabaseClient
from gmail_client import GmailClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Polling interval in seconds
POLLING_INTERVAL = 60

class EmailProcessor:
    def __init__(self, db_client: DatabaseClient, gmail_client: GmailClient):
        self.db_client = db_client
        self.gmail_client = gmail_client
        self.last_check_time = self._get_last_check_time()
        self.processed_message_ids = self._get_processed_message_ids()
        logger.info(f"Initial last check time: {self.last_check_time}")
        logger.info(f"Found {len(self.processed_message_ids)} processed message IDs")

    def _get_processed_message_ids(self) -> set:
        """Get all message IDs that are already in the database"""
        try:
            return self.db_client.get_all_message_ids()
        except Exception as e:
            logger.error(f"Error getting processed message IDs: {str(e)}")
            return set()

    def _get_last_check_time(self) -> datetime:
        """Get the timestamp of the most recent email in the database"""
        try:
            latest_email = self.db_client.get_latest_email()
            if latest_email and 'received_at' in latest_email:
                last_time = datetime.strptime(latest_email['received_at'], '%Y-%m-%d %H:%M:%S')
                logger.info(f"Latest email in database is from: {last_time}")
                return last_time
            logger.info("No emails found in database, starting from beginning")
            return datetime.min
        except Exception as e:
            logger.error(f"Error getting last check time: {str(e)}")
            return datetime.min

    def process_new_emails(self):
        """Process only new emails that haven't been seen before"""
        try:
            logger.info(f"Fetching emails newer than {self.last_check_time}")
            # Fetch only new emails since last check
            emails = self.gmail_client.fetch_emails(
                max_results=None,  # Fetch all emails
                after_date=self.last_check_time
            )
            
            if not emails:
                logger.info("No new emails found in Gmail")
                return

            # Filter out emails we've already processed
            new_emails = [
                email for email in emails 
                if email.get('message_id') not in self.processed_message_ids
            ]

            if not new_emails:
                logger.info("All found emails were already in the database")
                return

            logger.info(f"Found {len(emails)} total emails, {len(new_emails)} are new")

            # Store new emails
            stored_count = self.db_client.store_emails(new_emails)
            
            if stored_count > 0:
                # Update last check time to the newest email's date
                self.last_check_time = datetime.strptime(
                    new_emails[0]['received_at'], 
                    '%Y-%m-%d %H:%M:%S'
                )
                # Update processed message IDs
                self.processed_message_ids.update(
                    email.get('message_id') for email in new_emails
                )
                logger.info(f"Successfully stored {stored_count} new emails")
                logger.info(f"Newest email date: {new_emails[0].get('received_at', 'unknown')}")
                logger.info(f"Oldest email date: {new_emails[-1].get('received_at', 'unknown')}")
                logger.info(f"Updated last check time to: {self.last_check_time}")
            else:
                logger.info("No new emails to store")
                logger.info("This could be because:")
                logger.info("1. The emails were already in the database")
                logger.info("2. The emails were outside the date range")
                logger.info("3. There was an error storing the emails")
                # Log the message IDs to help debug
                if new_emails:
                    logger.info("Message IDs of emails that weren't stored:")
                    for email in new_emails:
                        logger.info(f"- {email.get('message_id', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Error processing new emails: {str(e)}")
            raise

def main():
    try:
        # Load credentials
        with open('credentials.yml', 'r') as f:
            credentials = yaml.safe_load(f)
        
        # Get user ID from environment variable or use default
        user_id = os.getenv('USER_ID', 'default_user')
        
        # Initialize clients
        gmail_client = GmailClient(user_id)
        db_client = DatabaseClient()
        
        # Initialize processor
        processor = EmailProcessor(db_client, gmail_client)
        
        logger.info("Starting email processing...")
        
        while True:
            try:
                # Get current email statistics
                stats = db_client.get_email_count()
                logger.info(f"Current email statistics:")
                logger.info(f"Total emails: {stats['total']}")
                logger.info(f"Unprocessed emails: {stats['unprocessed']}")
                logger.info(f"Processed emails: {stats['processed']}")
                
                # Process new emails
                processor.process_new_emails()
                
                # Show next check time
                next_check = datetime.now() + timedelta(seconds=POLLING_INTERVAL)
                logger.info(f"Next check for new emails at: {next_check.strftime('%Y-%m-%d %H:%M:%S')}")
                
                time.sleep(POLLING_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Stopping email processing...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                time.sleep(POLLING_INTERVAL)
                
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 