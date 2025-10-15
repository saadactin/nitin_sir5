@echo off
REM start_project.bat - Double-click this file to set up venv, install requirements and start the Flask app.
echo Starting project setup and launch...

REM Step 1: Create Python virtual environment if it doesn't exist
if not exist myenv\ (
    echo Creating virtual environment...
    python -m venv myenv
) else (
    echo Virtual environment already exists.
)

REM Step 2: Activate the virtual environment
echo Activating virtual environment...
call myenv\Scripts\activate.bat

REM Step 3: Install requirements
echo Installing required packages...
python -m pip install -r requirements.txt

REM Step 4: Start the Flask application
echo Starting Flask application...
start "" http://localhost:5000/login
python app.py
