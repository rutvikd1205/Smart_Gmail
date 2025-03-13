# Gmail Email Processor

A Python application that fetches emails from Gmail and stores them in MongoDB for processing. This project is designed to help you process and analyze your Gmail inbox programmatically.

## Features

- Fetches emails from Gmail using the Gmail API
- Stores emails in MongoDB for efficient retrieval and processing
- Configurable polling interval and batch size
- Handles email body decoding and parsing
- Supports both plain text and HTML email content

## Prerequisites

- Python 3.8 or higher
- MongoDB installed and running locally
- Gmail API credentials (OAuth 2.0)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/gmail-email-processor.git
cd gmail-email-processor
```

2. Create and activate a virtual environment:
```bash
python -m venv gmailreader_env
source gmailreader_env/bin/activate  # On Windows: gmailreader_env\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up Gmail API:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the Gmail API
   - Create OAuth 2.0 credentials
   - Download the credentials and save as `credentials.json` in the project root

5. Create a `.env` file in the project root:
```env
# MongoDB Settings
MONGODB_HOST=localhost
MONGODB_PORT=27017
MONGODB_DATABASE=email_db

# Gmail API Settings
GMAIL_CREDENTIALS_FILE=credentials.json
GMAIL_TOKEN_FILE=token.pickle

# Email Processing Settings
MAX_EMAILS_PER_FETCH=100
POLLING_INTERVAL=300
```

## Usage

1. Start MongoDB:
```bash
brew services start mongodb-community  # On macOS
```

2. Run the application:
```bash
python main.py
```

The first time you run the application, it will:
1. Open a browser window for Gmail authentication
2. Create the necessary database and collections in MongoDB
3. Start fetching and storing emails

## Project Structure

- `main.py`: Main application entry point
- `gmail_client.py`: Gmail API integration
- `db_client.py`: MongoDB database operations
- `.env`: Configuration file (not included in repository)
- `credentials.json`: Gmail API credentials (not included in repository)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Google Gmail API
- MongoDB
- Python community 