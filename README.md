# PSG Football Data ETL

This project extracts data about Paris Saint-Germain from the football-data.org API, transforms it, and loads it for analysis.Will vectorize data to create a RAG that ingests all PSG data to respond to any questions about it

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Get an API Key:
   - Go to [football-data.org](https://www.football-data.org/)
   - Register for a free account.
   - You will receive an API Token via email.

3. Configure Environment:
   - Create a file named `.env` in this directory.
   - Add your key: `API_KEY=your_api_key_here`
