"""Optional Sentry error reporting for the Python services (Edrak fork).

Inert unless SENTRY_DSN is set, so upstream behaviour is unchanged. Each service passes its name so
issues are attributed per process inside the shared all-in-one container.
"""

import os


def init_sentry(service_name: str) -> bool:
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if not dsn:
        return False
    import sentry_sdk

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("SENTRY_ENVIRONMENT", os.getenv("NODE_ENV", "production")),
        release=os.getenv("SENTRY_RELEASE") or None,
        server_name=service_name,
        traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0")),
        send_default_pii=False,
    )
    sentry_sdk.set_tag("service", service_name)
    return True
