import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from db_client import DatabaseClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def format_email(email):
    """Format email for display"""
    print("\n" + "="*80)
    print(f"Subject: {email['subject']}")
    print(f"From: {email['sender']}")
    print(f"Date: {email['received_at']}")
    print("-"*80)
    print("Body:")
    print("-"*80)
    print(email['body'])
    print("="*80 + "\n")

def get_emails_by_timeframe(collection, days: int) -> List[Dict]:
    """Get emails within specified number of days"""
    cutoff_date = datetime.now() - timedelta(days=days)
    return list(collection.find({
        'received_at': {'$gte': cutoff_date.strftime('%Y-%m-%d %H:%M:%S')}
    }, {'_id': 0}))

def get_emails_by_sender(collection, sender_email: str) -> List[Dict]:
    """Get emails from specific sender"""
    return list(collection.find({
        'sender': {'$regex': sender_email, '$options': 'i'}
    }, {'_id': 0}))

def get_emails_with_pagination(collection, page: int = 1, per_page: int = 10) -> List[Dict]:
    """Get emails with pagination"""
    skip = (page - 1) * per_page
    return list(collection.find({}, {'_id': 0})
               .sort('received_at', -1)
               .skip(skip)
               .limit(per_page))

def get_emails_by_status(collection, processed: bool) -> List[Dict]:
    """Get emails by processed status"""
    return list(collection.find({
        'processed': processed
    }, {'_id': 0}))

def display_email_list(emails: List[Dict], title: str = "Emails"):
    """Display list of emails with pagination"""
    if not emails:
        print(f"\nNo {title.lower()} found.")
        return

    print(f"\n{title} ({len(emails)}):")
    for i, email in enumerate(emails, 1):
        print(f"{i}. {email['subject']} - {email['sender']} - {email['received_at']}")

def main():
    try:
        # Connect to database
        db_client = DatabaseClient()
        collection = db_client.db.emails
        
        while True:
            print("\nEmail Retrieval Options:")
            print("1. Get all emails")
            print("2. Get emails from last 24 hours")
            print("3. Get emails from last 7 days")
            print("4. Get emails from last 30 days")
            print("5. Get emails from specific sender")
            print("6. Get unprocessed emails")
            print("7. Get processed emails")
            print("8. Get emails with pagination")
            print("9. Get emails by custom time range")
            print("10. Exit")
            
            choice = input("\nEnter your choice (1-10): ")
            
            if choice == '1':
                emails = list(collection.find({}, {'_id': 0}))
                display_email_list(emails, "All Emails")
                
            elif choice == '2':
                emails = get_emails_by_timeframe(collection, 1)
                display_email_list(emails, "Last 24 Hours Emails")
                
            elif choice == '3':
                emails = get_emails_by_timeframe(collection, 7)
                display_email_list(emails, "Last 7 Days Emails")
                
            elif choice == '4':
                emails = get_emails_by_timeframe(collection, 30)
                display_email_list(emails, "Last 30 Days Emails")
                
            elif choice == '5':
                sender = input("Enter sender email (or part of it): ")
                emails = get_emails_by_sender(collection, sender)
                display_email_list(emails, f"Emails from {sender}")
                
            elif choice == '6':
                emails = get_emails_by_status(collection, False)
                display_email_list(emails, "Unprocessed Emails")
                
            elif choice == '7':
                emails = get_emails_by_status(collection, True)
                display_email_list(emails, "Processed Emails")
                
            elif choice == '8':
                try:
                    page = int(input("Enter page number (1-10): "))
                    per_page = int(input("Enter emails per page (1-100): "))
                    emails = get_emails_with_pagination(collection, page, per_page)
                    display_email_list(emails, f"Page {page} Emails")
                except ValueError:
                    print("Please enter valid numbers!")
                    
            elif choice == '9':
                try:
                    days = int(input("Enter number of days: "))
                    emails = get_emails_by_timeframe(collection, days)
                    display_email_list(emails, f"Last {days} Days Emails")
                except ValueError:
                    print("Please enter a valid number!")
                    
            elif choice == '10':
                break
                
            else:
                print("Invalid choice! Please try again.")
            
            # Ask if user wants to view specific email
            if choice in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
                try:
                    view_email = input("\nWould you like to view a specific email? (y/n): ").lower()
                    if view_email == 'y':
                        index = int(input(f"Enter email index (1-{len(emails)}): ")) - 1
                        if 0 <= index < len(emails):
                            format_email(emails[index])
                        else:
                            print("Invalid index!")
                except ValueError:
                    print("Please enter valid numbers!")
                
    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        if 'db_client' in locals():
            db_client.close()

if __name__ == "__main__":
    main() 