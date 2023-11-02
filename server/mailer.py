from google.appengine.api import mail
from config import Config, ConfigTarget

def send_email(config: Config, to: str, subject: str, body: str):

  project_id = config.project_id
  sender = f"no-reply@{project_id}.appspotmail.com"

  #sender = os.environ['EMAIL_SENDER']  # replace with your email address
  message = mail.EmailMessage(sender=sender, subject=subject, to=to, body=body)
  message.send()
