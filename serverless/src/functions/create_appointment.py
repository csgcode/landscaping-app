import json
import os
import uuid
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Optional
from urllib.parse import urljoin
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

import boto3
import requests


ENV = os.getenv("ENVIRONMENT", "dev")
APPOINTMENTS_TABLE_NAME = os.getenv("APPOINTMENTS_TABLE_NAME")
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL")
USER_SERVICE_CLIENT_DETAIL = "/api/v1/clients/"
SERVICES_SERVICE_URL = os.getenv("SERVICES_SERVICE_URL")
SERVICES_SERVICE_DETAIL = "/api/v1/services/"
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "5"))


dynamodb = boto3.resource("dynamodb")
ddb_client = boto3.client("dynamodb")
appointments_table = dynamodb.Table(APPOINTMENTS_TABLE_NAME)

logger = logging.getLogger("create-appointment")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s", "correlation_id": "%(correlation_id)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def to_utc(dt_str: str) -> datetime:
    """Convert ISO-8601 datetime string to UTC datetime."""
    dt = datetime.fromisoformat(dt_str)
    return (dt if dt.tzinfo else dt.replace(tzinfo=ZoneInfo("UTC"))).astimezone(
        ZoneInfo("UTC")
    )


def response(
    status: int, body: dict, correlation_id: str, headers: dict | None = None
) -> dict:
    """Standardized API Gateway response."""
    base_headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
        "X-Correlation-Id": correlation_id,
    }
    if headers:
        base_headers.update(headers)

    body_with_correlation = {**body, "correlation_id": correlation_id}

    return {
        "statusCode": status,
        "headers": base_headers,
        "body": json.dumps(body_with_correlation),
    }


def get_header(event: dict, name: str) -> str | None:
    headers = event.get("headers") or {}
    name_lower = name.lower()
    for k, v in headers.items():
        if k.lower() == name_lower:
            return v
    return None


def validate_client(client_id: str, correlation_id: str) -> tuple[bool, Optional[dict], Optional[dict]]:
    """
    Validate client exists via User Service API.
    
    Returns:
        (exists, client_data, error_response)
    """

    try:
        url = urljoin(USER_SERVICE_URL, f"{USER_SERVICE_CLIENT_DETAIL}{client_id}")
        headers = {
            "X-Correlation-Id": correlation_id,
            "Accept": "application/json"
        }
        
        logger.info(
            "Calling User Service to validate client",
            extra={
                "correlation_id": correlation_id,
                "client_id": client_id,
                "url": url,
            },
        )
        
        response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        
        if response.status_code == 404:
            logger.warning(
                "Client not found in User Service",
                extra={
                    "correlation_id": correlation_id,
                    "client_id": client_id,
                    "status_code": response.status_code,
                },
            )
            return False, None, {
                "error": "Client not found",
                "client_id": client_id
            }
        
        if response.status_code != 200:
            logger.error(
                "User Service returned error",
                extra={
                    "correlation_id": correlation_id,
                    "client_id": client_id,
                    "status_code": response.status_code,
                    "response_body": response.text[:500],
                },
            )
            return False, None, {
                "error": "Failed to verify client",
                "details": f"User service returned status {response.status_code}"
            }
        
        client_data = response.json()
        logger.info(
            "Client validated successfully",
            extra={
                "correlation_id": correlation_id,
                "client_id": client_id,
            },
        )
        return True, client_data, None
        
    except requests.Timeout:
        logger.error(
            "User Service request timed out",
            extra={
                "correlation_id": correlation_id,
                "client_id": client_id,
                "timeout": API_TIMEOUT,
            },
            exc_info=True,
        )
        return False, None, {
            "error": "Service timeout",
            "details": "User service did not respond in time"
        }
    
    except Exception as e:
        logger.error(
            "Unexpected error validating client",
            extra={
                "correlation_id": correlation_id,
                "client_id": client_id,
                "error": str(e),
            },
            exc_info=True,
        )
        return False, None, {
            "error": "Internal error",
            "details": "Failed to validate client"
        }


def validate_service(service_id: str, correlation_id: str) -> tuple[bool, Optional[dict], Optional[dict]]:
    """
    Validate service exists via Services Service API.
    
    Returns:
        (exists, service_data, error_response)
    """
    if not SERVICES_SERVICE_URL:
        logger.error(
            "SERVICES_SERVICE_URL not configured",
            extra={"correlation_id": correlation_id},
        )
        return False, None, {
            "error": "Service configuration error",
            "details": "Services service unavailable"
        }

    try:
        url = urljoin(SERVICES_SERVICE_URL, f"{SERVICES_SERVICE_DETAIL}{service_id}")
        headers = {
            "X-Correlation-Id": correlation_id,
            "Accept": "application/json"
        }
        
        logger.info(
            "Calling Services Service to validate service",
            extra={
                "correlation_id": correlation_id,
                "service_id": service_id,
                "url": url,
            },
        )
        
        response = requests.get(url, headers=headers, timeout=API_TIMEOUT)
        
        if response.status_code == 404:
            logger.warning(
                "Service not found in Services Service",
                extra={
                    "correlation_id": correlation_id,
                    "service_id": service_id,
                    "status_code": response.status_code,
                },
            )
            return False, None, {
                "error": "Service not found",
                "service_id": service_id
            }
        
        if response.status_code != 200:
            logger.error(
                "Services Service returned error",
                extra={
                    "correlation_id": correlation_id,
                    "service_id": service_id,
                    "status_code": response.status_code,
                    "response_body": response.text[:500],
                },
            )
            return False, None, {
                "error": "Failed to verify service",
                "details": f"Services service returned status {response.status_code}"
            }
        
        service_data = response.json()
        logger.info(
            "Service validated successfully",
            extra={
                "correlation_id": correlation_id,
                "service_id": service_id,
            },
        )
        return True, service_data, None
        
    except requests.Timeout:
        logger.error(
            "Services Service request timed out",
            extra={
                "correlation_id": correlation_id,
                "service_id": service_id,
                "timeout": API_TIMEOUT,
            },
            exc_info=True,
        )
        return False, None, {
            "error": "Service timeout",
            "details": "Services service did not respond in time"
        }

    except Exception as e:
        logger.error(
            "Unexpected error validating service",
            extra={
                "correlation_id": correlation_id,
                "service_id": service_id,
                "error": str(e),
            },
            exc_info=True,
        )
        return False, None, {
            "error": "Internal error",
            "details": "Failed to validate service"
        }


def validate_payload(payload: dict, correlation_id: str) -> tuple[bool, dict | None]:
    """
    Validate appointment payload.
    
    Returns:
        (is_valid, error_response_or_none)
    """
    required_fields = ["client_id", "service_id", "scheduled_at", "location"]
    missing = [f for f in required_fields if f not in payload]
    if missing:
        logger.warning(
            "Missing required fields",
            extra={"correlation_id": correlation_id, "missing_fields": missing},
        )
        return False, response(
            400,
            {"error": "Missing required fields", "fields": missing},
            correlation_id,
        )

    try:
        to_utc(payload["scheduled_at"])
    except Exception as e:
        logger.warning(
            "Invalid scheduled_at format",
            extra={
                "correlation_id": correlation_id,
                "scheduled_at": payload.get("scheduled_at"),
                "error": str(e),
            },
        )
        return False, response(
            400,
            {"error": "scheduled_at must be valid ISO-8601 datetime"},
            correlation_id,
        )

    notes = payload.get("notes", "")
    if len(notes) > 1024:
        logger.warning(
            "Notes exceeds maximum length",
            extra={"correlation_id": correlation_id, "notes_length": len(notes)},
        )
        return False, response(
            400,
            {"error": "notes must not exceed 1024 characters"},
            correlation_id,
        )

    return True, None


def create_appointment(
    payload: dict, correlation_id: str
) -> tuple[int, dict]:
    """
    Create appointment
    
    Returns:
        (status_code, response_body)
    """
    client_id = payload["client_id"]
    service_id = payload["service_id"]
    notes = payload.get("notes", "")
    location = payload.get("location")

    scheduled_dt_utc = to_utc(payload["scheduled_at"])
    scheduled_iso = scheduled_dt_utc.isoformat()

    with ThreadPoolExecutor(max_workers=2) as executor:
        client_future = executor.submit(validate_client, client_id, correlation_id)
        service_future = executor.submit(validate_service, service_id, correlation_id)
        
        client_exists, client_data, client_error = client_future.result()
        service_exists, service_data, service_error = service_future.result()

    if not client_exists:
        return 404 if "not found" in client_error["error"].lower() else 503, client_error

    if not service_exists:
        return 404 if "not found" in service_error["error"].lower() else 503, service_error


    appointment_id = f"apt_{uuid.uuid4().hex}"
    gsi_date_pk = scheduled_iso[:10].replace("-", "")
    created_at = datetime.now(timezone.utc).isoformat()

    appt_item = {
        "appointment_id": {"S": appointment_id},
        "client_id": {"S": client_id},
        "service_id": {"S": service_id},
        "scheduled_at": {"S": scheduled_iso},
        "gsi_date_pk": {"S": gsi_date_pk},
        "location": {"S": location},
        "status": {"S": "scheduled"},
        "notes": {"S": notes},
        "weather_risk": {"S": "unknown"},
        "correlation_id": {"S": correlation_id},
        "created_at": {"S": created_at},
    }

    uniq_key = f"UNIQ#{client_id}#{service_id}#{scheduled_iso}#{location}"
    uniq_item = {
        "appointment_id": {"S": uniq_key},
        "type": {"S": "uniqueness_marker"},
        "ref_appointment_id": {"S": appointment_id},
        "created_at": {"S": created_at},
    }

    # Atomic transaction to create uniqueness check + create
    try:
        ddb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": APPOINTMENTS_TABLE_NAME,
                        "Item": uniq_item,
                        "ConditionExpression": "attribute_not_exists(appointment_id)",
                    }
                },
                {
                    "Put": {
                        "TableName": APPOINTMENTS_TABLE_NAME,
                        "Item": appt_item,
                        "ConditionExpression": "attribute_not_exists(appointment_id)",
                    }
                },
            ]
        )
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("TransactionCanceledException", "ConditionalCheckFailedException"):
            logger.warning(
                "Duplicate appointment detected",
                extra={
                    "correlation_id": correlation_id,
                    "client_id": client_id,
                    "service_id": service_id,
                    "scheduled_at": scheduled_iso,
                },
            )
            return 409, {
                "error": "Appointment already exists for this client, service, and time slot"
            }
        
        logger.error(
            "Failed to create appointment - DynamoDB error",
            extra={
                "correlation_id": correlation_id,
                "error_code": error_code,
                "error_message": str(e),
            },
            exc_info=True,
        )
        return 500, {"error": "Failed to create appointment"}

    logger.info(
        "Appointment created successfully",
        extra={
            "correlation_id": correlation_id,
            "appointment_id": appointment_id,
            "client_id": client_id,
            "service_id": service_id,
            "scheduled_at": scheduled_iso,
        },
    )

    return 201, {
        "appointment_id": appointment_id,
        "status": "scheduled",
        "scheduled_at": scheduled_iso,
        "location": location,
    }


def handler(event: dict, context: Any) -> dict:
    """
    Lambda handler for creating appointments.
    
    Expects:
        - Headers: X-Correlation-Id (optional)
        - Body: JSON with client_id, service_id, scheduled_at, location, notes (optional)
    
    Returns:
        API Gateway response with status code and body
    """
    # Generate or extract correlation ID
    correlation_id = get_header(event, "X-Correlation-Id") or f"cor-{uuid.uuid4().hex}"
    
    logger.info(
        "Received create appointment request",
        extra={"correlation_id": correlation_id},
    )

    try:
        raw_body = event.get("body") or "{}"
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        logger.warning(
            "Invalid JSON in request body",
            extra={
                "correlation_id": correlation_id,
                "error": str(e),
                "body_preview": raw_body[:200],
            },
        )
        return response(400, {"error": "Invalid JSON body"}, correlation_id)

    is_valid, error_response = validate_payload(payload, correlation_id)
    if not is_valid:
        return error_response

    status_code, body = create_appointment(payload, correlation_id)
    return response(status_code, body, correlation_id)