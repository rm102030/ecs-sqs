import random
import time

from opentelemetry import trace

tracer = trace.get_tracer(__name__)

def send_sms(payload):

    with tracer.start_as_current_span(
        "twilio-send-message"
    ) as span:

        print()
        print("======================")
        print("TWILIO REQUEST")
        print("======================")

        print(f"to: {payload['recipient']}")
        print(f"message: {payload['message']}")

        span.set_attribute(
            "provider.name",
            "Twilio"
        )

        span.set_attribute(
            "provider.channel",
            "SMS"
        )

        span.set_attribute(
            "provider.recipient",
            payload["recipient"]
        )

        # ============================================
        # Simulated Provider Latency
        # ============================================

        time.sleep(1)

        # ============================================
        # Simulated Provider Behaviors
        # ============================================

        simulated = random.choice([
            "SUCCESS",
            "SUCCESS",
            "SUCCESS",
            "TIMEOUT",
            "RATE_LIMIT",
            "PROVIDER_DOWN"
        ])

        # ============================================
        # TIMEOUT
        # ============================================

        if simulated == "TIMEOUT":

            span.set_attribute(
                "provider.error",
                "timeout"
            )

            print()
            print("======================")
            print("TWILIO ERROR")
            print("======================")
            print("Twilio timeout")

            raise Exception(
                "Twilio timeout"
            )

        # ============================================
        # RATE LIMIT
        # ============================================

        if simulated == "RATE_LIMIT":

            span.set_attribute(
                "provider.error",
                "rate_limit"
            )

            print()
            print("======================")
            print("TWILIO ERROR")
            print("======================")
            print("429 Too Many Requests")

            raise Exception(
                "429 Too Many Requests"
            )

        # ============================================
        # PROVIDER DOWN
        # ============================================

        if simulated == "PROVIDER_DOWN":

            span.set_attribute(
                "provider.error",
                "provider_down"
            )

            print()
            print("======================")
            print("TWILIO ERROR")
            print("======================")
            print("503 Provider Unavailable")

            raise Exception(
                "503 Provider Unavailable"
            )

        # ============================================
        # SUCCESS
        # ============================================

        response = {

            "provider": "Twilio",

            "providerMessageId": "SM-123456",

            "status": "DELIVERED"
        }

        span.set_attribute(
            "provider.status",
            "DELIVERED"
        )

        print()
        print("======================")
        print("TWILIO RESPONSE")
        print("======================")

        print(response)

        return response
