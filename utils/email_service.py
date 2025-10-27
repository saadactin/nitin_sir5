import os
import smtplib
import ssl
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Dict
from pathlib import Path

# Load environment variables early, if python-dotenv is available
try:
    from dotenv import load_dotenv
    # Try to load .env from current directory and parent directory
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()  # Try current directory
    
    # Also try email.env if it exists
    email_env_path = Path(__file__).parent.parent / 'email.env'
    if email_env_path.exists():
        load_dotenv(email_env_path)
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _split_emails(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [e.strip() for e in value.split(",") if e.strip()]


@dataclass
class SendResult:
    success: bool
    error: Optional[str] = None
    attempts: int = 0
    alert_type: Optional[str] = None
    subject: Optional[str] = None
    recipients: Optional[List[str]] = None
    timestamp: datetime = datetime.utcnow()


class SystemAlertEmailService:
    """
    Comprehensive email service for system alerts with:
    - Multiple alert types (sync_failed, server_down, system_error, high_resource, sync_completed)
    - HTML and plain text templates
    - Retry mechanism with exponential backoff (3 attempts)
    - Rate limiting (max 5 emails per hour per alert type per server)
    - Circuit breaker (open after 3 consecutive failures; cool down 15 minutes)
    - Optional async support (thread-based; Celery-ready hook)
    - Delivery status tracking to email_delivery.log
    - Comprehensive error handling
    """

    def __init__(self):
        self._config_loaded = False
        self._lock = threading.Lock()  # Initialize lock first
        self._consecutive_failures = 0
        self._circuit_open_until: Optional[datetime] = None
        self._send_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self._load_config()
        
    def _load_config(self):
        """Load configuration from environment variables"""
        # Load .env if not already loaded
        try:
            # Prefer python-dotenv when available
            from dotenv import load_dotenv
            env_path = Path(__file__).parent.parent / '.env'
            if env_path.exists():
                load_dotenv(env_path, override=False)
            else:
                load_dotenv(override=False)
            # Also try email.env if present
            email_env_path = Path(__file__).parent.parent / 'email.env'
            if email_env_path.exists():
                load_dotenv(email_env_path, override=False)
        except Exception:
            # Fallback: manually parse simple .env files into os.environ
            try:
                def _parse_env_file(p: Path):
                    try:
                        with p.open('r', encoding='utf-8') as fh:
                            for raw in fh:
                                line = raw.strip()
                                if not line or line.startswith('#'):
                                    continue
                                if '=' not in line:
                                    continue
                                k, v = line.split('=', 1)
                                k = k.strip()
                                v = v.strip().strip('"').strip("'")
                                # don't override already-set env vars
                                if k and v and os.environ.get(k) is None:
                                    os.environ[k] = v
                    except Exception:
                        pass

                env_path = Path(__file__).parent.parent / '.env'
                email_env_path = Path(__file__).parent.parent / 'email.env'
                if env_path.exists():
                    _parse_env_file(env_path)
                if email_env_path.exists():
                    _parse_env_file(email_env_path)
            except Exception:
                # If all else fails, continue; variables may already be present in environment
                pass
            
        # Configuration using Django-like env names with SMTP_* fallback
        self.email_host: str = os.getenv("EMAIL_HOST", os.getenv("SMTP_HOST", "smtp.gmail.com"))
        self.email_port: int = int(os.getenv("EMAIL_PORT", os.getenv("SMTP_PORT", "587")))
        self.email_use_tls: bool = _env_bool("EMAIL_USE_TLS", True)
        self.email_use_ssl: bool = _env_bool("EMAIL_USE_SSL", False)
        self.email_user: Optional[str] = os.getenv("EMAIL_HOST_USER", os.getenv("SMTP_USER"))
        self.email_password: Optional[str] = os.getenv("EMAIL_HOST_PASSWORD", os.getenv("SMTP_PASS"))
        self.default_from: str = os.getenv("DEFAULT_FROM_EMAIL", self.email_user or "noreply@example.com")
        self.admin_emails: List[str] = _split_emails(os.getenv("ADMIN_EMAILS"))
        
        # Log configuration for debugging
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"Email service config loaded: user={self.email_user}, admin_emails={self.admin_emails}")

        # Rate limit settings
        self._rate_limit_max_per_hour = int(os.getenv("EMAIL_RATE_LIMIT_PER_HOUR", "5"))

        # Circuit breaker settings
        self._circuit_failure_threshold = int(os.getenv("EMAIL_CIRCUIT_FAIL_THRESHOLD", "3"))
        self._circuit_cooldown_minutes = int(os.getenv("EMAIL_CIRCUIT_COOLDOWN_MINUTES", "15"))
        
        # Delivery log
        self._delivery_log_path = os.getenv("EMAIL_DELIVERY_LOG", "email_delivery.log")
        
        self._config_loaded = True

    def ensure_config_loaded(self):
        """Ensure configuration is loaded, reload if admin_emails is empty"""
        if not self.admin_emails:
            self._load_config()

    # --------------------- Public Notification APIs ---------------------
    def notify_sync_failed(self, server_name: str, error_message: str, recipients: Optional[List[str]] = None) -> SendResult:
        subject = f"[SYNC][FAILED] {server_name}"
        context = {"server_name": server_name, "error": error_message}
        return self._send_alert(
            alert_type="sync_failed",
            subject=subject,
            html_body=self._render_html("sync_failed", context),
            text_body=self._render_text("sync_failed", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"sync_failed:{server_name}")

    def notify_sync_success(self, server_name: str, summary: Optional[str] = None, recipients: Optional[List[str]] = None) -> SendResult:
        subject = f"[SYNC][COMPLETED] {server_name}"
        context = {"server_name": server_name, "summary": summary or "Sync completed successfully."}
        return self._send_alert(
            alert_type="sync_completed",
            subject=subject,
            html_body=self._render_html("sync_completed", context),
            text_body=self._render_text("sync_completed", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"sync_completed:{server_name}")

    def notify_server_down(self, server_name: str, error_message: str, recipients: Optional[List[str]] = None) -> SendResult:
        subject = f"[ALERT][SERVER DOWN] {server_name}"
        context = {"server_name": server_name, "error": error_message}
        return self._send_alert(
            alert_type="server_down",
            subject=subject,
            html_body=self._render_html("server_down", context),
            text_body=self._render_text("server_down", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"server_down:{server_name}")

    def notify_system_error(self, title: str, details: str, recipients: Optional[List[str]] = None) -> SendResult:
        subject = f"[SYSTEM ERROR] {title}"
        context = {"title": title, "details": details}
        return self._send_alert(
            alert_type="system_error",
            subject=subject,
            html_body=self._render_html("system_error", context),
            text_body=self._render_text("system_error", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key="system_error:global")

    def notify_row_count_mismatch(self, server_name: str, table_name: str, source_rows: int, target_rows: int, 
                                   percentage_diff: float, recipients: Optional[List[str]] = None) -> SendResult:
        """Notify when row counts differ significantly between source and target"""
        subject = f"[DATA MISMATCH] {server_name} - {table_name}"
        context = {
            "server_name": server_name,
            "table_name": table_name,
            "source_rows": f"{source_rows:,}",
            "target_rows": f"{target_rows:,}",
            "difference": f"{abs(target_rows - source_rows):,}",
            "percentage": f"{abs(percentage_diff):.2f}%",
            "status": "More" if target_rows > source_rows else "Fewer"
        }
        return self._send_alert(
            alert_type="row_count_mismatch",
            subject=subject,
            html_body=self._render_html("row_count_mismatch", context),
            text_body=self._render_text("row_count_mismatch", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"row_mismatch:{server_name}:{table_name}")

    def notify_sync_partial_success(self, server_name: str, total_tables: int, success_count: int, 
                                     failed_tables: List[str], error_summary: str, 
                                     recipients: Optional[List[str]] = None) -> SendResult:
        """Notify when sync completes with some failures"""
        subject = f"[SYNC][PARTIAL] {server_name} - {success_count}/{total_tables} tables synced"
        context = {
            "server_name": server_name,
            "total_tables": total_tables,
            "success_count": success_count,
            "failed_count": total_tables - success_count,
            "success_rate": f"{(success_count/total_tables*100):.1f}%" if total_tables > 0 else "0%",
            "failed_tables": ", ".join(failed_tables[:10]),  # Show first 10
            "more_failures": len(failed_tables) - 10 if len(failed_tables) > 10 else 0,
            "error_summary": error_summary
        }
        return self._send_alert(
            alert_type="sync_partial",
            subject=subject,
            html_body=self._render_html("sync_partial", context),
            text_body=self._render_text("sync_partial", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"sync_partial:{server_name}")

    def notify_daily_sync_summary(self, date: str, total_syncs: int, successful_syncs: int, failed_syncs: int,
                                   total_rows_synced: int, servers_summary: List[Dict], top_errors: List[str],
                                   recipients: Optional[List[str]] = None) -> SendResult:
        """Send daily summary of all sync operations"""
        subject = f"[DAILY SUMMARY] {date} - {successful_syncs}/{total_syncs} syncs successful"
        context = {
            "date": date,
            "total_syncs": total_syncs,
            "successful_syncs": successful_syncs,
            "failed_syncs": failed_syncs,
            "success_rate": f"{(successful_syncs/total_syncs*100):.1f}%" if total_syncs > 0 else "0%",
            "total_rows": f"{total_rows_synced:,}",
            "servers_summary": servers_summary,  # List of {server, syncs, status}
            "top_errors": top_errors[:5]  # Top 5 errors
        }
        return self._send_alert(
            alert_type="daily_summary",
            subject=subject,
            html_body=self._render_html("daily_summary", context),
            text_body=self._render_text("daily_summary", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"daily_summary:{date}")

    def notify_user_created(self, username: str, role: str, created_by: str, 
                            recipients: Optional[List[str]] = None) -> SendResult:
        """Notify when a new user is created in the system"""
        subject = f"[USER CREATED] New {role.upper()} user: {username}"
        context = {
            "username": username,
            "role": role,
            "created_by": created_by,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        return self._send_alert(
            alert_type="user_created",
            subject=subject,
            html_body=self._render_html("user_created", context),
            text_body=self._render_text("user_created", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"user_created:{username}")

    def notify_schedule_success(self, server_name: str, job_type: str, duration: Optional[str] = None,
                                 recipients: Optional[List[str]] = None) -> SendResult:
        """Notify when a scheduled sync completes successfully"""
        subject = f"[SCHEDULE][SUCCESS] {server_name} - {job_type}"
        context = {
            "server_name": server_name,
            "job_type": job_type,
            "duration": duration or "N/A",
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        return self._send_alert(
            alert_type="schedule_success",
            subject=subject,
            html_body=self._render_html("schedule_success", context),
            text_body=self._render_text("schedule_success", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"schedule_success:{server_name}:{job_type}")

    def notify_schedule_failed(self, server_name: str, job_type: str, error_message: str,
                                recipients: Optional[List[str]] = None) -> SendResult:
        """Notify when a scheduled sync fails"""
        subject = f"[SCHEDULE][FAILED] {server_name} - {job_type}"
        context = {
            "server_name": server_name,
            "job_type": job_type,
            "error": error_message,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        return self._send_alert(
            alert_type="schedule_failed",
            subject=subject,
            html_body=self._render_html("schedule_failed", context),
            text_body=self._render_text("schedule_failed", context),
            recipients=recipients or (self.admin_emails or [self.email_user] if self.email_user else []),
            rate_key=f"schedule_failed:{server_name}:{job_type}")

    # --------------------- Core Send Logic ---------------------
    def _send_alert(self, alert_type: str, subject: str, html_body: str, text_body: str, recipients: List[str], rate_key: str) -> SendResult:
        # Ensure config is loaded (will reload if admin_emails is empty)
        self.ensure_config_loaded()
        
        now = datetime.utcnow()

        # Prepare result default
        result = SendResult(success=False, alert_type=alert_type, subject=subject, recipients=recipients, attempts=0)

        if not recipients:
            result.error = "No recipients configured"
            self._log_delivery(result)
            return result

        # Circuit breaker check
        with self._lock:
            if self._circuit_open_until and now < self._circuit_open_until:
                result.error = f"Circuit open until {self._circuit_open_until.isoformat()}"
                self._log_delivery(result)
                return result

            # Rate limit per hour per rate_key
            history = self._send_history[rate_key]
            one_hour_ago = now - timedelta(hours=1)
            while history and history[0] < one_hour_ago:
                history.popleft()
            if len(history) >= self._rate_limit_max_per_hour:
                result.error = f"Rate limited: {len(history)} sent in last hour"
                self._log_delivery(result)
                return result

        # Retry with exponential backoff
        last_error: Optional[str] = None
        for attempt in range(1, 4):  # 3 attempts
            result.attempts = attempt
            try:
                self._smtp_send(subject, html_body, text_body, recipients)
                # Success: record history
                with self._lock:
                    self._send_history[rate_key].append(datetime.utcnow())
                    self._consecutive_failures = 0
                    self._circuit_open_until = None
                result.success = True
                self._log_delivery(result)
                return result
            except Exception as e:
                last_error = str(e)
                time.sleep(2 ** (attempt - 1))

        # All attempts failed -> update circuit breaker
        with self._lock:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self._circuit_failure_threshold:
                self._circuit_open_until = datetime.utcnow() + timedelta(minutes=self._circuit_cooldown_minutes)
        result.error = last_error or "Unknown send error"
        self._log_delivery(result)
        return result

    def _smtp_send(self, subject: str, html_body: str, text_body: str, recipients: List[str]) -> None:
        if not self.email_host or not self.email_user or not self.email_password:
            raise RuntimeError("Email not configured: EMAIL_HOST/USER/PASSWORD required")

        msg = MIMEMultipart("alternative")
        msg["From"] = self.default_from
        msg["To"] = ",".join(recipients)
        msg["Subject"] = subject
        if text_body:
            msg.attach(MIMEText(text_body, "plain"))
        if html_body:
            msg.attach(MIMEText(html_body, "html"))

        if self.email_use_ssl or self.email_port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.email_host, self.email_port, context=context, timeout=20) as server:
                server.login(self.email_user, self.email_password)
                server.sendmail(self.default_from, recipients, msg.as_string())
        else:
            with smtplib.SMTP(self.email_host, self.email_port, timeout=20) as server:
                if self.email_use_tls:
                    server.starttls(context=ssl.create_default_context())
                server.login(self.email_user, self.email_password)
                server.sendmail(self.default_from, recipients, msg.as_string())

    # --------------------- Rendering ---------------------
    def _render_html(self, template: str, ctx: Dict) -> str:
        base_style = "font-family: Arial, sans-serif; color:#222;"
        header = f"<div style='background:#0a66c2;color:#fff;padding:10px 14px;border-radius:6px 6px 0 0;'><strong>System Notification</strong></div>"
        footer = "<div style='font-size:12px;color:#666;margin-top:8px;'>This is an automated message.</div>"
        body = ""
        
        if template == "sync_failed":
            body = f"<p style='{base_style}'>Sync <strong>FAILED</strong> for <strong>{ctx.get('server_name','')}</strong>.</p>" \
                   f"<pre style='background:#fee;border:1px solid #f99;padding:8px;border-radius:4px;white-space:pre-wrap;'>{ctx.get('error','')}</pre>"
        
        elif template == "sync_completed":
            body = f"<p style='{base_style}'>Sync <strong>COMPLETED</strong> for <strong>{ctx.get('server_name','')}</strong>.</p>" \
                   f"<p style='{base_style}'>{ctx.get('summary','')}</p>"
        
        elif template == "server_down":
            body = f"<p style='{base_style}'><strong>SERVER DOWN</strong>: <strong>{ctx.get('server_name','')}</strong></p>" \
                   f"<pre style='background:#fee;border:1px solid #f99;padding:8px;border-radius:4px;white-space:pre-wrap;'>{ctx.get('error','')}</pre>"
        
        elif template == "system_error":
            body = f"<p style='{base_style}'><strong>{ctx.get('title','System Error')}</strong></p>" \
                   f"<pre style='background:#fee;border:1px solid #f99;padding:8px;border-radius:4px;white-space:pre-wrap;'>{ctx.get('details','')}</pre>"
        
        elif template == "row_count_mismatch":
            body = f"<p style='{base_style}'><strong>⚠️ DATA MISMATCH DETECTED</strong></p>" \
                   f"<p style='{base_style}'>Server: <strong>{ctx.get('server_name','')}</strong><br>" \
                   f"Table: <strong>{ctx.get('table_name','')}</strong></p>" \
                   f"<div style='background:#fff3cd;border:1px solid #ffc107;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<table style='width:100%;border-collapse:collapse;'>" \
                   f"<tr><td style='padding:4px;'><strong>Source Rows:</strong></td><td style='text-align:right;'>{ctx.get('source_rows','0')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Target Rows:</strong></td><td style='text-align:right;'>{ctx.get('target_rows','0')}</td></tr>" \
                   f"<tr style='border-top:1px solid #ccc;'><td style='padding:4px;'><strong>Difference:</strong></td><td style='text-align:right;color:#d9534f;'>{ctx.get('difference','0')} ({ctx.get('percentage','0')})</td></tr>" \
                   f"</table></div>" \
                   f"<p style='{base_style};font-size:13px;'><strong>Status:</strong> {ctx.get('status','')} rows in target than source</p>"
        
        elif template == "sync_partial":
            body = f"<p style='{base_style}'><strong>⚠️ PARTIAL SYNC COMPLETED</strong></p>" \
                   f"<p style='{base_style}'>Server: <strong>{ctx.get('server_name','')}</strong></p>" \
                   f"<div style='background:#d1ecf1;border:1px solid #17a2b8;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<p style='margin:0;'><strong>Success Rate:</strong> {ctx.get('success_rate','0%')}</p>" \
                   f"<p style='margin:5px 0 0 0;'><strong>✓ Successful:</strong> {ctx.get('success_count',0)} tables</p>" \
                   f"<p style='margin:5px 0 0 0;'><strong>✗ Failed:</strong> {ctx.get('failed_count',0)} tables</p>" \
                   f"</div>" \
                   f"<p style='{base_style};'><strong>Failed Tables:</strong></p>" \
                   f"<pre style='background:#fee;border:1px solid #f99;padding:8px;border-radius:4px;font-size:12px;'>{ctx.get('failed_tables','None')}" \
                   f"{(' + ' + str(ctx.get('more_failures',0)) + ' more...') if ctx.get('more_failures',0) > 0 else ''}</pre>" \
                   f"<p style='{base_style};font-size:13px;'><strong>Error Summary:</strong><br>{ctx.get('error_summary','No details available')}</p>"
        
        elif template == "daily_summary":
            servers_html = ""
            for srv in ctx.get('servers_summary', []):
                status_color = "#28a745" if srv.get('status') == 'success' else "#dc3545"
                servers_html += f"<tr><td style='padding:6px;border-bottom:1px solid #eee;'>{srv.get('server','')}</td>" \
                               f"<td style='padding:6px;border-bottom:1px solid #eee;text-align:center;'>{srv.get('syncs',0)}</td>" \
                               f"<td style='padding:6px;border-bottom:1px solid #eee;text-align:center;color:{status_color};'><strong>{srv.get('status','').upper()}</strong></td></tr>"
            
            errors_html = ""
            for err in ctx.get('top_errors', []):
                errors_html += f"<li style='margin:5px 0;'>{err}</li>"
            
            body = f"<p style='{base_style}'><strong>📊 DAILY SYNC SUMMARY</strong></p>" \
                   f"<p style='{base_style}'>Date: <strong>{ctx.get('date','')}</strong></p>" \
                   f"<div style='background:#d4edda;border:1px solid #28a745;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<table style='width:100%;border-collapse:collapse;'>" \
                   f"<tr><td style='padding:4px;'><strong>Total Syncs:</strong></td><td style='text-align:right;'>{ctx.get('total_syncs',0)}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>✓ Successful:</strong></td><td style='text-align:right;color:#28a745;'>{ctx.get('successful_syncs',0)}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>✗ Failed:</strong></td><td style='text-align:right;color:#dc3545;'>{ctx.get('failed_syncs',0)}</td></tr>" \
                   f"<tr style='border-top:1px solid #ccc;'><td style='padding:4px;'><strong>Success Rate:</strong></td><td style='text-align:right;'><strong>{ctx.get('success_rate','0%')}</strong></td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Total Rows Synced:</strong></td><td style='text-align:right;'>{ctx.get('total_rows','0')}</td></tr>" \
                   f"</table></div>" \
                   f"<p style='{base_style};margin-top:15px;'><strong>Server Summary:</strong></p>" \
                   f"<table style='width:100%;border-collapse:collapse;background:#f8f9fa;'>" \
                   f"<tr style='background:#e9ecef;'><th style='padding:8px;text-align:left;'>Server</th><th style='padding:8px;text-align:center;'>Syncs</th><th style='padding:8px;text-align:center;'>Status</th></tr>" \
                   f"{servers_html}</table>" \
                   f"{'<p style=\"' + base_style + ';margin-top:15px;\"><strong>Top Errors:</strong></p><ul style=\"font-size:13px;\">' + errors_html + '</ul>' if errors_html else ''}"
        
        elif template == "user_created":
            role_color = "#0a66c2" if ctx.get('role') == 'admin' else "#17a2b8" if ctx.get('role') == 'operator' else "#6c757d"
            body = f"<p style='{base_style}'><strong>👤 NEW USER CREATED</strong></p>" \
                   f"<div style='background:#e7f3ff;border:1px solid #0a66c2;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<table style='width:100%;border-collapse:collapse;'>" \
                   f"<tr><td style='padding:4px;'><strong>Username:</strong></td><td style='text-align:right;'>{ctx.get('username','')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Role:</strong></td><td style='text-align:right;color:{role_color};'><strong>{ctx.get('role','').upper()}</strong></td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Created By:</strong></td><td style='text-align:right;'>{ctx.get('created_by','')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Timestamp:</strong></td><td style='text-align:right;'>{ctx.get('timestamp','')}</td></tr>" \
                   f"</table></div>" \
                   f"<p style='{base_style};font-size:13px;'>A new user account has been created in the system. Please review if this was expected.</p>"
        
        elif template == "schedule_success":
            body = f"<p style='{base_style}'><strong>✓ SCHEDULED SYNC COMPLETED</strong></p>" \
                   f"<p style='{base_style}'>Server: <strong>{ctx.get('server_name','')}</strong></p>" \
                   f"<div style='background:#d4edda;border:1px solid #28a745;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<table style='width:100%;border-collapse:collapse;'>" \
                   f"<tr><td style='padding:4px;'><strong>Job Type:</strong></td><td style='text-align:right;'>{ctx.get('job_type','')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Duration:</strong></td><td style='text-align:right;'>{ctx.get('duration','')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Completed At:</strong></td><td style='text-align:right;'>{ctx.get('timestamp','')}</td></tr>" \
                   f"</table></div>" \
                   f"<p style='{base_style};font-size:13px;'>The scheduled synchronization completed successfully.</p>"
        
        elif template == "schedule_failed":
            body = f"<p style='{base_style}'><strong>✗ SCHEDULED SYNC FAILED</strong></p>" \
                   f"<p style='{base_style}'>Server: <strong>{ctx.get('server_name','')}</strong></p>" \
                   f"<div style='background:#f8d7da;border:1px solid #dc3545;padding:12px;border-radius:4px;margin:10px 0;'>" \
                   f"<table style='width:100%;border-collapse:collapse;'>" \
                   f"<tr><td style='padding:4px;'><strong>Job Type:</strong></td><td style='text-align:right;'>{ctx.get('job_type','')}</td></tr>" \
                   f"<tr><td style='padding:4px;'><strong>Failed At:</strong></td><td style='text-align:right;'>{ctx.get('timestamp','')}</td></tr>" \
                   f"</table></div>" \
                   f"<p style='{base_style};'><strong>Error Details:</strong></p>" \
                   f"<pre style='background:#fee;border:1px solid #f99;padding:8px;border-radius:4px;white-space:pre-wrap;font-size:12px;'>{ctx.get('error','')}</pre>" \
                   f"<p style='{base_style};font-size:13px;color:#dc3545;'>The scheduled synchronization encountered an error. Please investigate.</p>"
        
        else:
            body = f"<p style='{base_style}'>Notification: {template}</p>"
        
        container = f"<div style='border:1px solid #ddd;border-radius:6px;overflow:hidden'>{header}<div style='padding:12px'>{body}{footer}</div></div>"
        return container

    def _render_text(self, template: str, ctx: Dict) -> str:
        if template == "sync_failed":
            return f"Sync FAILED for {ctx.get('server_name','')}\n\nError:\n{ctx.get('error','')}"
        
        if template == "sync_completed":
            return f"Sync COMPLETED for {ctx.get('server_name','')}\n{ctx.get('summary','')}"
        
        if template == "server_down":
            return f"SERVER DOWN: {ctx.get('server_name','')}\n\nError:\n{ctx.get('error','')}"
        
        if template == "system_error":
            return f"SYSTEM ERROR: {ctx.get('title','')}\n\n{ctx.get('details','')}"
        
        if template == "row_count_mismatch":
            return f"DATA MISMATCH DETECTED\n\n" \
                   f"Server: {ctx.get('server_name','')}\n" \
                   f"Table: {ctx.get('table_name','')}\n\n" \
                   f"Source Rows: {ctx.get('source_rows','0')}\n" \
                   f"Target Rows: {ctx.get('target_rows','0')}\n" \
                   f"Difference: {ctx.get('difference','0')} ({ctx.get('percentage','0')})\n\n" \
                   f"Status: {ctx.get('status','')} rows in target than source"
        
        if template == "sync_partial":
            return f"PARTIAL SYNC COMPLETED\n\n" \
                   f"Server: {ctx.get('server_name','')}\n" \
                   f"Success Rate: {ctx.get('success_rate','0%')}\n" \
                   f"Successful: {ctx.get('success_count',0)} tables\n" \
                   f"Failed: {ctx.get('failed_count',0)} tables\n\n" \
                   f"Failed Tables:\n{ctx.get('failed_tables','None')}" \
                   f"{(' + ' + str(ctx.get('more_failures',0)) + ' more...') if ctx.get('more_failures',0) > 0 else ''}\n\n" \
                   f"Error Summary:\n{ctx.get('error_summary','No details available')}"
        
        if template == "daily_summary":
            servers_text = "\n".join([f"  - {srv.get('server','')}: {srv.get('syncs',0)} syncs ({srv.get('status','').upper()})" 
                                      for srv in ctx.get('servers_summary', [])])
            errors_text = "\n".join([f"  - {err}" for err in ctx.get('top_errors', [])])
            
            return f"DAILY SYNC SUMMARY\n\n" \
                   f"Date: {ctx.get('date','')}\n\n" \
                   f"Total Syncs: {ctx.get('total_syncs',0)}\n" \
                   f"Successful: {ctx.get('successful_syncs',0)}\n" \
                   f"Failed: {ctx.get('failed_syncs',0)}\n" \
                   f"Success Rate: {ctx.get('success_rate','0%')}\n" \
                   f"Total Rows Synced: {ctx.get('total_rows','0')}\n\n" \
                   f"Server Summary:\n{servers_text}\n" \
                   f"{('\\nTop Errors:\\n' + errors_text) if errors_text else ''}"
        
        if template == "user_created":
            return f"NEW USER CREATED\n\n" \
                   f"Username: {ctx.get('username','')}\n" \
                   f"Role: {ctx.get('role','').upper()}\n" \
                   f"Created By: {ctx.get('created_by','')}\n" \
                   f"Timestamp: {ctx.get('timestamp','')}\n\n" \
                   f"A new user account has been created in the system. Please review if this was expected."
        
        if template == "schedule_success":
            return f"SCHEDULED SYNC COMPLETED\n\n" \
                   f"Server: {ctx.get('server_name','')}\n" \
                   f"Job Type: {ctx.get('job_type','')}\n" \
                   f"Duration: {ctx.get('duration','')}\n" \
                   f"Completed At: {ctx.get('timestamp','')}\n\n" \
                   f"The scheduled synchronization completed successfully."
        
        if template == "schedule_failed":
            return f"SCHEDULED SYNC FAILED\n\n" \
                   f"Server: {ctx.get('server_name','')}\n" \
                   f"Job Type: {ctx.get('job_type','')}\n" \
                   f"Failed At: {ctx.get('timestamp','')}\n\n" \
                   f"Error Details:\n{ctx.get('error','')}\n\n" \
                   f"The scheduled synchronization encountered an error. Please investigate."
        
        return f"Notification: {template}"

    # --------------------- Logging ---------------------
    def _log_delivery(self, result: SendResult) -> None:
        try:
            line = {
                "timestamp": result.timestamp.isoformat(),
                "success": result.success,
                "error": result.error,
                "attempts": result.attempts,
                "alert_type": result.alert_type,
                "subject": result.subject,
                "recipients": ",".join(result.recipients or []),
            }
            with open(self._delivery_log_path, "a", encoding="utf-8") as f:
                f.write(str(line) + "\n")
        except Exception:
            pass

    # --------------------- Async Hook (Celery-ready) ---------------------
    def send_async(self, func, *args, **kwargs):
        """
        Minimal async support. If Celery is configured externally, wire this to a Celery task.
        Otherwise, run in a daemon thread.
        """
        t = threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True)
        t.start()
        return t


# Singleton instance to import elsewhere
email_service = SystemAlertEmailService()


