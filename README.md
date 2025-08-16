# Sanctions-Screening-Public-Project
A powerful, multilingual, open-source sanctions screening tool designed to run locally. It allows analysts and compliance teams to quickly, privately, and effectively check names and entities against major international sanctions lists (OFAC, UN, EU, UK). 
Key Features
List Aggregator: Automatically downloads and consolidates sanctions lists from OFAC, the UN, the European Union, and the United Kingdom into a single local SQLite database.

Intelligent Search (Fuzzy Search): Finds matches even with typos, name inversions (e.g., "Nicolas Maduro" vs. "Maduro Moros, Nicolas"), or minor variations, thanks to its powerful fuzzy search engine.

Advanced Filters: Refine searches by date of birth, nationality, and identifiers (passport, ID number, etc.), with the option to exclude aliases to reduce false positives.

Adjustable Sensitivity: Control the strictness of the search algorithm with a sensitivity slider to tailor the results to your needs.

Multilingual Interface: Full support for English, Spanish, French, German, Russian, Dutch, and Arabic, with automatic adaptation for right-to-left (RTL) languages.

100% Local and Private: The entire process, from the database to the search interface, runs on your own machine. No search data is sent over the internet.

Export Results: Download your search results in CSV format for your records, audits, or for sharing.

🛠️ Tech Stack
Backend: Python, Flask, TheFuzz

Database: SQLite

Frontend: HTML5, Tailwind CSS, Vanilla JavaScript

Key Libraries: requests, lxml, Flask-Cors

🚀 Installation and Usage Guide
Follow these steps to get the project up and running on your local machine.

Prerequisites
Python 3.8 or higher must be installed. You can check your version with the command:

python --version

1. Clone the Repository
Open your terminal, navigate to the folder where you want to save the project, and clone the repository:

git clone [https://github.com/your-username/your-repository.git](https://github.com/your-username/your-repository.git)
cd your-repository

2. Create a Virtual Environment (Recommended)
It is good practice to isolate the project's dependencies.

# Create the environment
python -m venv venv

# Activate it
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

3. Install Dependencies
Install all the necessary libraries from the requirements.txt file.

pip install -r requirements.txt

(Note: Make sure you have a requirements.txt file with the following content: requests, psycopg2-binary, python-dotenv, Flask, Flask-Cors, thefuzz)

4. Generate the Database
The first step is to run the ofac_parser.py script to download the sanctions lists and build the local sanctions_lists.db database.

python ofac_parser.py

This process may take several minutes the first time, as it is downloading and processing thousands of records.

5. Start the Server
Once the database has been created, start the local web server with Flask:

python server.py

If everything goes well, you will see a message in the terminal indicating that the server is running at http://127.0.0.1:5001.

6. Use the Tool!
Open your web browser and go to the following address:

http://127.0.0.1:5001

That's it! You can now start performing searches.

📂 Project Structure
/your-repository
|
├── ofac_parser.py          # Script to download and process sanctions lists.
├── server.py               # Flask web server that acts as the backend and API.
├── verificador_final.html  # The frontend file you see in the browser.
├── sanctions_lists.db      # The SQLite database (generated after running the parser).
├── requirements.txt        # List of Python dependencies.
└── README.md               # This file.

📄 License
This project is licensed under the MIT License. See the LICENSE file for details.

⚖️ Disclaimer
This tool is provided "as is" and is intended for informational and assistance purposes only. It should not be the sole source for making legal, financial, or compliance decisions. Always verify the results against the official sources of the sanctions lists.
