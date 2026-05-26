from providers.twilio_provider import (
    send_sms
)

def send_notification(payload):

    channel = payload["channel"]

    if channel == "SMS":

        return send_sms(payload)

    if channel == "EMAIL":

        return {
            "provider": "MockEmail",
            "status": "DELIVERED"
        }

    raise Exception(
        f"Unsupported channel: {channel}"
    )
