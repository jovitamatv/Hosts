import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def send_email(subject, body):
    """
    Send email notification for errors.
    
    Args:
        subject (str): Email subject
        body (str): Email body content

    Returns:
        bool: True if email sent successfully, False otherwise.
    """

    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    recipient_email = os.getenv("ALERT_EMAIL")
    
    if not all([smtp_user, smtp_password, recipient_email]):
        return False
    
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = recipient_email
        msg['Subject'] = subject
        
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(smtp_server, int(smtp_port)) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        return True
        
    except Exception as e:
        print(f"Failed to send email notification: {e}")
        return False