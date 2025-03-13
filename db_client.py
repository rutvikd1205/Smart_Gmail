import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError, ConnectionFailure
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class DatabaseClient:
    def __init__(self):
        self.client = None
        self.db = None
        self.emails = None
        self.connect()
        self._create_indexes()

    def connect(self):
        """Connect to MongoDB"""
        try:
            # Get MongoDB connection details from environment variables
            host = os.getenv('MONGODB_HOST', 'localhost')
            port = int(os.getenv('MONGODB_PORT', '27017'))
            username = os.getenv('MONGODB_USER', '')
            password = os.getenv('MONGODB_PASSWORD', '')
            database = os.getenv('MONGODB_DB', 'email_db')

            # Print connection details (without password) for debugging
            logger.info(f"Connecting to MongoDB at {host}:{port}")
            logger.info(f"Database: {database}")
            logger.info(f"Username: {username}")

            # Create MongoDB connection string
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/"
            if not username or not password:
                connection_string = f"mongodb://{host}:{port}/"

            # Connect to MongoDB
            self.client = MongoClient(connection_string)
            self.db = self.client[database]
            self.emails = self.db['emails']

            # Test the connection
            self.client.server_info()
            logger.info("Successfully connected to MongoDB")

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to MongoDB: {str(e)}")
            raise

    def _create_indexes(self):
        """Create necessary indexes for email collection"""
        try:
            self.emails.create_index([('message_id', 1)], unique=True)
            self.emails.create_index([('processed', 1)])
            self.emails.create_index([('received_at', -1)])
            logger.info("Successfully created database indexes")
        except Exception as e:
            logger.error(f"Error creating indexes: {str(e)}")
            raise

    def get_latest_email(self) -> Optional[Dict]:
        """Get the most recent email from the database"""
        try:
            return self.emails.find_one(
                sort=[('received_at', -1)]
            )
        except Exception as e:
            logger.error(f"Error getting latest email: {str(e)}")
            return None

    def store_emails(self, emails: List[Dict]) -> int:
        """Store emails in the database, skipping duplicates"""
        try:
            stored_count = 0
            for email in emails:
                try:
                    # Check if email already exists
                    existing = self.emails.find_one({'message_id': email['message_id']})
                    
                    if existing:
                        # Email exists, skip it
                        continue
                    
                    # New email, mark as processed since we've seen it
                    email['processed'] = True
                    
                    # Insert new email
                    self.emails.insert_one(email)
                    stored_count += 1
                    
                except Exception as e:
                    logger.error(f"Error storing email {email.get('message_id', 'unknown')}: {str(e)}")
                    continue
            
            return stored_count
        except Exception as e:
            logger.error(f"Error storing emails: {str(e)}")
            return 0

    def get_email_count(self) -> Dict[str, int]:
        """Get email statistics"""
        try:
            total = self.emails.count_documents({})
            unprocessed = self.emails.count_documents({'processed': False})
            processed = self.emails.count_documents({'processed': True})
            
            return {
                'total': total,
                'unprocessed': unprocessed,
                'processed': processed
            }
        except Exception as e:
            logger.error(f"Error getting email count: {str(e)}")
            return {'total': 0, 'unprocessed': 0, 'processed': 0}

    def get_all_message_ids(self) -> set:
        """Get all message IDs that are already in the database"""
        try:
            # Get all message IDs from the database
            message_ids = self.emails.distinct('message_id')
            return set(message_ids)
        except Exception as e:
            logger.error(f"Error getting all message IDs: {str(e)}")
            return set()

    def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close()
            logger.info("Database connection closed") 