import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from pymongo import MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

class DatabaseClient:
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize DatabaseClient with MongoDB configuration from environment variables
        
        :param logger: Optional logging.Logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.client = None
        self.db = None
        self.mongo_config = {
            'host': os.getenv('MONGODB_HOST', 'localhost'),
            'port': int(os.getenv('MONGODB_PORT', '27017')),
            'database': os.getenv('MONGODB_DATABASE', 'email_db'),
            'username': os.getenv('MONGODB_USERNAME'),
            'password': os.getenv('MONGODB_PASSWORD')
        }
        self.connect()

    def validate_config(self) -> None:
        """
        Validate MongoDB configuration environment variables
        
        :raises ValueError: If any required environment variable is missing
        """
        required_vars = ['host', 'port', 'database']
        for key in required_vars:
            if not self.mongo_config.get(key):
                error_msg = f"Environment variable MONGODB_{key.upper()} is not set"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

    def connect(self):
        """Connect to the MongoDB database"""
        try:
            self.validate_config()
            
            # Create connection string
            connection_string = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}"
            
            # Add authentication if credentials are provided
            if self.mongo_config.get('username') and self.mongo_config.get('password'):
                connection_string = f"mongodb://{self.mongo_config['username']}:{self.mongo_config['password']}@{self.mongo_config['host']}:{self.mongo_config['port']}"
            
            self.logger.info(f"Attempting to connect to MongoDB at {self.mongo_config['host']}:{self.mongo_config['port']}")
            
            self.client = MongoClient(connection_string)
            self.db = self.client[self.mongo_config['database']]
            
            # Test the connection
            self.client.server_info()
            self.logger.info("Successfully connected to MongoDB database")

        except PyMongoError as e:
            self.logger.error(f"Error connecting to database: {str(e)}")
            raise

    def store_emails(self, emails: List[Dict]) -> int:
        """Store emails in the database"""
        if not emails:
            return 0

        stored_count = 0
        try:
            collection = self.db.emails
            
            for email in emails:
                try:
                    # Ensure body is properly decoded if it's bytes
                    body = email['body']
                    if isinstance(body, bytes):
                        body = body.decode('utf-8', errors='replace')

                    # Prepare document for MongoDB
                    document = {
                        'message_id': email['message_id'],
                        'subject': email['subject'],
                        'sender': email['sender'],
                        'received_at': email['received_at'],
                        'body': body,
                        'raw_email': email['raw_email'],
                        'processed': False,
                        'created_at': datetime.utcnow()
                    }

                    # Use upsert to avoid duplicates
                    result = collection.update_one(
                        {'message_id': email['message_id']},
                        {'$set': document},
                        upsert=True
                    )
                    
                    if result.upserted_id or result.modified_count:
                        stored_count += 1

                except Exception as e:
                    self.logger.error(f"Error storing email {email['message_id']}: {str(e)}")
                    continue

            return stored_count

        except PyMongoError as e:
            self.logger.error(f"Error in store_emails: {str(e)}")
            return 0

    def get_unprocessed_emails(self, limit: int = 100) -> List[Dict]:
        """Retrieve unprocessed emails from the database"""
        try:
            collection = self.db.emails
            return list(collection.find(
                {'processed': False},
                {'_id': 0}  # Exclude MongoDB's _id field
            ).sort('received_at', 1).limit(limit))

        except PyMongoError as e:
            self.logger.error(f"Error retrieving unprocessed emails: {str(e)}")
            return []

    def mark_emails_processed(self, email_ids: List[str]) -> bool:
        """Mark emails as processed"""
        if not email_ids:
            return True

        try:
            collection = self.db.emails
            result = collection.update_many(
                {'message_id': {'$in': email_ids}},
                {'$set': {'processed': True}}
            )
            return result.modified_count > 0

        except PyMongoError as e:
            self.logger.error(f"Error marking emails as processed: {str(e)}")
            return False

    def close(self):
        """Close the database connection"""
        if self.client:
            self.client.close() 