@echo off
REM start_project.bat - Double-click this file to set up venv, install requirements and start the Flask app.
SET SCRIPT_DIR=%~dp0
Powershell -NoProfile -ExecutionPolicy Bypass -Command "& '%SCRIPT_DIR%setup_and_run.ps1'"
