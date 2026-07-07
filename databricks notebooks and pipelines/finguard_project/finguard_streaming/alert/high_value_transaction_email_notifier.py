from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def create_alert_email_body(alert_data):
    """Creates the HTML email body for a high-value transaction alert."""
    return f"""<html><body style="font-family: Arial, sans-serif;">
<h2 style="color: #d9534f;">⚠️ High Value Transaction Alert</h2>
<p>Dear {alert_data['customer_name']},</p>
<p>We detected a transaction that exceeded your configured limit:</p>
<div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #d9534f; margin: 20px 0;">
<h3 style="margin-top: 0;">Transaction Details</h3>
<ul style="list-style-type: none; padding-left: 0;">
<li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
<li><strong>Amount:</strong> {alert_data['currency']} {alert_data['transaction_amount']:,.2f}</li>
<li><strong>Your Limit:</strong> {alert_data['currency']} {alert_data['transaction_limit']:,.2f}</li>
<li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
<li><strong>Location:</strong> {alert_data['city']}, {alert_data['country']}</li>
<li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
</ul></div>
<p><strong>Action Required:</strong></p>
<ul>
<li>If you authorized this transaction, no action is needed.</li>
<li>If you did NOT authorize this, contact our fraud department immediately.</li>
</ul>
<p>For your security, we're monitoring your account closely.</p>
<hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
<p style="font-size: 12px; color: #6c757d;">
Alert ID: {alert_data['alert_id']}<br>
This is an automated message from FinGuard Transaction Monitoring System.
</p>
</body></html>"""


# Get configuration outside the foreach_batch_sink for serialization
EMAIL_FROM = "databeli13@gmail.com"
try:
    APP_PASSWORD = dbutils().secrets().get("finguard-scope", "gmail_api_key")
except Exception as e:
    print(f"❌ Failed to retrieve Gmail API key from secrets: {e}")
    APP_PASSWORD = None


@dp.foreach_batch_sink(name="email_notifier_sink")
def send_high_value_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for high-value transactions."""
    
    if APP_PASSWORD is None:
        print(f"❌ Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return
    
    rows = df.collect()
    print(f"📧 Batch {batch_id}: Processing {len(rows)} alert(s)...")
    
    success_count = 0
    failure_count = 0
    
    for row in rows:
        try:
            alert_data = {
                'alert_id': row.alert_id,
                'customer_name': row.customer_name,
                'transaction_id': row.transaction_id,
                'transaction_amount': float(row.transaction_amount),
                'currency': row.currency,
                'transaction_limit': float(row.transaction_limit),
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_timestamp': str(row.transaction_timestamp),
                'city': row.city,
                'country': row.country
            }
            
            subject = f"⚠️ High Value Transaction Alert - {alert_data['alert_id']}"
            body = create_alert_email_body(alert_data)
            
            send_email(row.customer_email, subject, body, EMAIL_FROM, APP_PASSWORD)
            
            success_count += 1
            print(f"  ✅ Email sent to {row.customer_email} for transaction {row.transaction_id}")
            
        except Exception as e:
            failure_count += 1
            print(f"  ❌ Error processing alert {row.alert_id}: {e}")
    
    print(f"📊 Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")


@dp.append_flow(target="email_notifier_sink")
def high_value_alert_stream():
    """Streaming flow that reads high-value transaction alerts."""
    return spark.readStream.table("finguard.gold.high_value_transactions_alert")
