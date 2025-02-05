import requests
from bs4 import BeautifulSoup
import openai
import sqlite3
import json
import os
from datetime import datetime

# ----------------------------------------------------------
# 1. Configuration and First Principles Setup
# ----------------------------------------------------------

# Ensure that the OpenAI API key is set
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    raise Exception("OPENAI_API_KEY environment variable not set.")

# Define the SQLite database file (data storage layer)
DB_FILENAME = "vc_data.db"

# ----------------------------------------------------------
# 2. Data Ingestion Layer: Fetching and Parsing Web Data
# ----------------------------------------------------------

def fetch_web_page(url):
    """Fetch raw HTML content from the given URL using first principles HTTP request."""
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching {url}: {response.status_code}")
    return response.text

def parse_html_to_text(html_content):
    """Parse HTML and extract plain text content."""
    soup = BeautifulSoup(html_content, "html.parser")
    # Remove script and style elements for cleaner text
    for script in soup(["script", "style"]):
        script.decompose()
    return soup.get_text(separator="\n")

# ----------------------------------------------------------
# 3. Data Extraction Layer: Using AI to Structure Information
# ----------------------------------------------------------

def extract_vc_data(text):
    """
    Using GPT (via OpenAI API), extract structured VC data from raw text.
    We instruct the model from first principles: the output must be a JSON array with objects
    containing: Name, Location, Website, Focus.
    """
    prompt = f"""
You are an expert data extractor. Your task is to analyze the following text and extract a list of venture capital (VC) firms.
For each VC firm, provide:
- "Name": The name of the VC firm.
- "Location": The headquarters or main location.
- "Website": The website URL.
- "Focus": The sectors or industries the firm invests in (if available).
Output the result as a JSON array where each element is an object with these keys.

Text to analyze:
{text}
"""
    # Call the ChatCompletion API
    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an expert data extractor."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.0,
        max_tokens=1500
    )
    reply = response['choices'][0]['message']['content']
    try:
        vc_list = json.loads(reply)
    except Exception as e:
        print("Failed to parse GPT response. Raw reply:")
        print(reply)
        raise e
    return vc_list

# ----------------------------------------------------------
# 4. Data Storage Layer: SQLite Database Setup and Operations
# ----------------------------------------------------------

def initialize_database():
    """Set up the SQLite database and create the table for VC data if it doesn't exist."""
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vc_firms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            location TEXT,
            website TEXT,
            focus TEXT,
            last_updated TEXT
        )
    """)
    conn.commit()
    conn.close()

def insert_vc_records(vc_records):
    """Insert each VC record into the database with a current timestamp."""
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()
    for record in vc_records:
        cursor.execute("""
            INSERT INTO vc_firms (name, location, website, focus, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (
            record.get("Name", ""),
            record.get("Location", ""),
            record.get("Website", ""),
            record.get("Focus", ""),
            now
        ))
    conn.commit()
    conn.close()
    print(f"Inserted {len(vc_records)} records into the database.")

def query_all_vc_records():
    """Retrieve and print all VC records for verification."""
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, location, website, focus, last_updated FROM vc_firms")
    rows = cursor.fetchall()
    conn.close()
    print("Current VC Records:")
    for row in rows:
        print(row)

# ----------------------------------------------------------
# 5. Main Routine: Integrate All Components
# ----------------------------------------------------------

def main():
    # Initialize the data storage
    initialize_database()
    
    # Step A: Ingest the source data
    source_url = input("Enter the URL of a VC directory page: ").strip()
    print("Fetching web page content...")
    html = fetch_web_page(source_url)
    
    print("Parsing HTML to extract text...")
    text = parse_html_to_text(html)
    
    # Step B: Use AI to extract structured data
    print("Extracting VC data from text using AI...")
    vc_data = extract_vc_data(text)
    print("Extracted Data:")
    print(json.dumps(vc_data, indent=2))
    
    # Step C: Save the structured data into the database
    print("Saving data into the database...")
    insert_vc_records(vc_data)
    
    # Optional: Query database to verify insertion
    query_all_vc_records()

if __name__ == "__main__":
    main()
