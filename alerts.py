import re
from datetime import datetime, timedelta

import os

class LogAnalyzer:
    def __init__(self, log_file_path):
        self.log_file_path = log_file_path
        self.alerts = []
        self.warnings = []
        self.infos = []

    def parse_logs(self):
        """Parse the log file and categorize messages by severity (only last 7 days)"""
        if not os.path.exists(self.log_file_path):
            print(f"Error: Log file not found at {self.log_file_path}")
            return

        seven_days_ago = datetime.now() - timedelta(days=7)

        with open(self.log_file_path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue

                # Try multiple timestamp patterns
                # Pattern 1: YYYY-MM-DD HH:MM:SS (sync_operations.log format)
                pattern1 = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) - (\w+) - \[.*?\] - (.*)'
                # Pattern 2: YYYY-MM-DD HH:MM:SS,mmm (old format)
                pattern2 = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - (\w+) - (.*)'
                
                match = re.match(pattern1, line) or re.match(pattern2, line)

                if match:
                    timestamp_str, level, message = match.groups()
                    try:
                        # Try new format first (no milliseconds)
                        try:
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        except ValueError:
                            # Try old format with milliseconds
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                    except ValueError:
                        timestamp = datetime.now()

                    # Only include entries from the last 7 days
                    if timestamp < seven_days_ago:
                        continue

                    log_entry = {
                        'timestamp': timestamp,
                        'level': level,
                        'message': message,
                        'raw_line': line
                    }

                    # Categorize based on level and message content
                    if level == 'ERROR' or 'error' in message.lower() or 'SYNC_FAILED' in message:
                        self.alerts.append(log_entry)
                    elif level == 'WARNING' or 'warning' in message.lower() or 'failed' in message.lower() or 'SYNC_PARTIAL' in message:
                        self.warnings.append(log_entry)
                    else:
                        self.infos.append(log_entry)
                else:
                    # Non-matching lines - check if within date range
                    # Try to extract any date pattern
                    date_pattern = re.search(r'(\d{4}-\d{2}-\d{2})', line)
                    if date_pattern:
                        try:
                            date_str = date_pattern.group(1)
                            log_date = datetime.strptime(date_str, '%Y-%m-%d')
                            if log_date.date() >= (datetime.now() - timedelta(days=7)).date():
                                log_entry = {
                                    'timestamp': datetime.now(),
                                    'level': 'INFO',
                                    'message': line,
                                    'raw_line': line
                                }
                                self.infos.append(log_entry)
                        except ValueError:
                            pass
    
    def generate_html_report(self, output_file='alerts.html'):
        """Generate HTML report with categorized alerts"""
        self.parse_logs()
        
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PostgreSQL Load Log Analysis</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 20px;
                    background-color: #f5f5f5;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    background: white;
                    padding: 20px;
                    border-radius: 8px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header {{
                    background: #2c3e50;
                    color: white;
                    padding: 20px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .section {{
                    margin-bottom: 30px;
                    border: 1px solid #ddd;
                    border-radius: 5px;
                    overflow: hidden;
                }}
                .section-header {{
                    padding: 15px;
                    font-weight: bold;
                    font-size: 18px;
                }}
                .alert-header {{ background-color: #e74c3c; color: white; }}
                .warning-header {{ background-color: #f39c12; color: white; }}
                .info-header {{ background-color: #3498db; color: white; }}
                .log-entry {{
                    padding: 10px 15px;
                    border-bottom: 1px solid #eee;
                    font-family: monospace;
                    font-size: 14px;
                }}
                .log-entry:last-child {{
                    border-bottom: none;
                }}
                .timestamp {{
                    color: #666;
                    font-weight: bold;
                    margin-right: 10px;
                }}
                .level {{
                    padding: 2px 6px;
                    border-radius: 3px;
                    font-size: 12px;
                    font-weight: bold;
                    margin-right: 10px;
                }}
                .level-error {{ background-color: #e74c3c; color: white; }}
                .level-warning {{ background-color: #f39c12; color: white; }}
                .level-info {{ background-color: #3498db; color: white; }}
                .stats {{
                    background: #ecf0f1;
                    padding: 15px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .summary {{
                    display: flex;
                    justify-content: space-around;
                    flex-wrap: wrap;
                }}
                .stat-box {{
                    background: white;
                    padding: 15px;
                    border-radius: 5px;
                    text-align: center;
                    min-width: 150px;
                    margin: 5px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                .stat-number {{
                    font-size: 24px;
                    font-weight: bold;
                }}
                .alert-stat {{ color: #e74c3c; }}
                .warning-stat {{ color: #f39c12; }}
                .info-stat {{ color: #3498db; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>PostgreSQL Load Log Analysis</h1>
                    <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>Log file: {self.log_file_path}</p>
                </div>
                
                <div class="stats">
                    <h2>Summary Statistics</h2>
                    <div class="summary">
                        <div class="stat-box">
                            <div class="stat-number alert-stat">{len(self.alerts)}</div>
                            <div>Alerts/Errors</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-number warning-stat">{len(self.warnings)}</div>
                            <div>Warnings</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-number info-stat">{len(self.infos)}</div>
                            <div>Info Messages</div>
                        </div>
                        <div class="stat-box">
                            <div class="stat-number">{len(self.alerts) + len(self.warnings) + len(self.infos)}</div>
                            <div>Total Entries</div>
                        </div>
                    </div>
                </div>
        """
        
        # Alerts section
        if self.alerts:
            html_content += """
                <div class="section">
                    <div class="section-header alert-header">ALERTS/ERRORS</div>
            """
            for entry in self.alerts:
                html_content += f"""
                    <div class="log-entry">
                        <span class="timestamp">{entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</span>
                        <span class="level level-error">{entry['level']}</span>
                        {entry['message']}
                    </div>
                """
            html_content += "</div>"
        else:
            html_content += """
                <div class="section">
                    <div class="section-header alert-header">No Alerts/Errors Found</div>
                    <div class="log-entry">No error-level messages detected in the logs.</div>
                </div>
            """
        
        # Warnings section
        if self.warnings:
            html_content += """
                <div class="section">
                    <div class="section-header warning-header">WARNINGS</div>
            """
            for entry in self.warnings:
                html_content += f"""
                    <div class="log-entry">
                        <span class="timestamp">{entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</span>
                        <span class="level level-warning">{entry['level']}</span>
                        {entry['message']}
                    </div>
                """
            html_content += "</div>"
        else:
            html_content += """
                <div class="section">
                    <div class="section-header warning-header">No Warnings Found</div>
                    <div class="log-entry">No warning-level messages detected in the logs.</div>
                </div>
            """
        
        # Info section (limited to last 50 entries to avoid huge file)
        html_content += """
                <div class="section">
                    <div class="section-header info-header">RECENT INFO MESSAGES (Last 50)</div>
        """
        for entry in self.infos[-50:]:
            html_content += f"""
                    <div class="log-entry">
                        <span class="timestamp">{entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</span>
                        <span class="level level-info">{entry['level']}</span>
                        {entry['message']}
                    </div>
            """
        html_content += "</div>"
        
        html_content += """
            </div>
        </body>
        </html>
        """
        
        with open(output_file, 'w') as f:
            f.write(html_content)
        
        print(f"HTML report generated: {output_file}")
        print(f"Summary: {len(self.alerts)} alerts, {len(self.warnings)} warnings, {len(self.infos)} info messages")

def main():
    # Assuming the log file is in the same directory as this script
    log_file = 'load_postgres.log'
    
    if not os.path.exists(log_file):
        print(f"Error: Log file '{log_file}' not found in current directory.")
        print("Please ensure the log file exists at the same level as this script.")
        return
    
    analyzer = LogAnalyzer(log_file)
    analyzer.generate_html_report('alerts.html')

if __name__ == "__main__":
    main()