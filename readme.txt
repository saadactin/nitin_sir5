🚀 Setup Instructions
Step 1: Install dependencies

On Windows, follow these steps:

Install the SQL Server ODBC driver:
Open PowerShell and run:

winget install Microsoft.DataAccess.OdbcDriverForSqlServer


Install Python dependencies:
Run the following command in your project directory:

pip install -r requirements.txt

Step 2: Run the application

Start the Flask app by running:

python app.py


Your app should now be up and running!


5. Why This Fails Sometimes
SQL Browser service not running

Firewall blocking UDP 1434

Network policies restricting dynamic port discovery

6. The Registry Storage
Windows stores named instance port information in:

text
HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Microsoft SQL Server\MSSQL15.SQL2019_Second\MSSQLServer\SuperSocketNetLib\Tcp\IPAll
Where it specifies TCPDynamicPorts or TCPPort

I am facing an issue activating the Cursor Pro plan. While the bank has set up a standing instruction for the payment, Cursor has not received the funds.
Cursor support needs the following transaction details from our end to resolve this:
•	The last 4 digits of the company card used.
•	The exact date and time the charge was attempted.
	The card's 
