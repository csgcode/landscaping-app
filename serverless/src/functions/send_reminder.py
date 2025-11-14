import os, json, logging, uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

TEMPLATES_TABLE = os.getenv("TEMPLATES_TABLE_NAME")
PREFERENCES_TABLE = os.getenv("PREFERENCES_TABLE_NAME")
SEND_LOGS_TABLE = os.getenv("SEND_LOGS_TABLE_NAME")
SES_FROM_EMAIL = os.getenv("SES_FROM_EMAIL", "no-reply@example.com")
SMS_SENDER_ID = os.getenv("SMS_SENDER_ID", "LANDSCAPE")
NOTIFICATIONS_TOPIC_ARN = os.getenv("NOTIFICATIONS_TOPIC_ARN")

dynamo = boto3.resource("dynamodb")
templates_tbl = dynamo.Table(TEMPLATES_TABLE)
prefs_tbl = dynamo.Table(PREFERENCES_TABLE)
sendlogs_tbl = dynamo.Table(SEND_LOGS_TABLE)
ses = boto3.client("ses")
sns = boto3.client("sns")

log = logging.getLogger("send-reminder")
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def get_prefs(user_id: str) -> Dict[str, Any]:
    resp = prefs_tbl.get_item(Key={"user_id": user_id})
    return resp.get("Item") or {}

def get_template(template_id: str, channel: str, locale: str = "en_GB") -> Dict[str, Any]:
    # Simplest: use (template_id, version='v1') or query by GSI channel+locale if you need
    resp = templates_tbl.get_item(Key={"template_id": template_id, "version": "v1"})
    return resp.get("Item") or {"subject": "Reminder", "body": "Your appointment is coming up."}

def put_sendlog_once(notification_id: str, item: Dict[str, Any]) -> bool:
    try:
        sendlogs_tbl.put_item(
            Item={"notification_id": notification_id, **item},
            ConditionExpression="attribute_not_exists(notification_id)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("ConditionalCheckFailedException",):
            return False
        raise

def send_email(to_addr: str, subject: str, body: str) -> str:
    resp = ses.send_email(
        Source=SES_FROM_EMAIL,
        Destination={"ToAddresses": [to_addr]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body}},
        },
    )
    return resp["MessageId"]

def send_sms(phone_e164: str, body: str) -> str:
    resp = sns.publish(PhoneNumber=phone_e164, Message=body, MessageAttributes={
        "AWS.SNS.SMS.SenderID": {"DataType": "String", "StringValue": SMS_SENDER_ID}
    })
    return resp["MessageId"]

def handler(event, context):
    records: List[Dict[str, Any]] = event.get("Records", [])
    for r in records:
        try:
            body = json.loads(r.get("body") or "{}")
        except Exception:
            log.warning("Invalid JSON; skipping"); continue

        notif_id = body.get("notification_id") or f"ntf_{uuid.uuid4().hex}"
        user_id = body.get("user_id")
        notif_type = body.get("type", "appointment.reminder")
        correlation_id = body.get("correlation_id") or f"req-{uuid.uuid4().hex}"
        # expected: appointment_id, scheduled_at, maybe pre-render variables...
        variables = body.get("variables") or {}

        if not user_id:
            log.warning("Missing user_id; skipping");  continue

        created = put_sendlog_once(notif_id, {
            "user_id": user_id,
            "type": notif_type,
            "status": "processing",
            "attempts": 0,
            "correlation_id": correlation_id,
            "created_at": utcnow_iso(),
            "sent_at": "1970-01-01T00:00:00Z",
            "ttl": int(datetime.now(timezone.utc).timestamp()) + 90*24*3600,
        })
        if not created:
            log.info(f"Duplicate notification_id {notif_id}; skip");  continue

        # Resolve preferences (very simple example)
        prefs = get_prefs(user_id)
        channels_allowed = []
        # Global toggles
        ch = (prefs.get("channels") or {})
        if ch.get("email"): channels_allowed.append("email")
        if ch.get("sms"):   channels_allowed.append("sms")
        if ch.get("push"):  channels_allowed.append("push")
        # Type overrides
        type_over = (prefs.get("types") or {}).get(notif_type, {})
        channels_allowed = [c for c in channels_allowed if type_over.get(c, True)]

        # Load a template (naive: same subject/body for all)
        tpl = get_template("appointment_reminder_v1", "email", prefs.get("locale","en_GB"))
        subject = tpl.get("subject","Appointment reminder")
        body_txt = tpl.get("body","Your appointment is coming up.")

        # Fake contact data for skeleton demo: in real flow you’d read your Contacts read-model here
        email = body.get("email")           # or pull from Contacts table
        phone = body.get("phone_e164")      # or pull from Contacts table

        sent_any = False
        provider_ids = {}

        try:
            if "email" in channels_allowed and email:
                pid = send_email(email, subject, body_txt)
                provider_ids["email_msg_id"] = pid
                sent_any = True
            if "sms" in channels_allowed and phone:
                pid = send_sms(phone, body_txt)
                provider_ids["sms_msg_id"] = pid
                sent_any = True

            sendlogs_tbl.update_item(
                Key={"notification_id": notif_id},
                UpdateExpression="SET #s = :s, attempts = attempts + :one, provider = :p, sent_at = :t",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "sent" if sent_any else "skipped",
                                           ":one": 1,
                                           ":p": provider_ids,
                                           ":t": utcnow_iso()}
            )

        except Exception as e:
            # Let SQS retry by raising after updating log
            sendlogs_tbl.update_item(
                Key={"notification_id": notif_id},
                UpdateExpression="SET #s = :s, attempts = attempts + :one, last_error = :e",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":s": "failed", ":one": 1, ":e": str(e)},
            )
            raise

    return {"status": "ok"}
