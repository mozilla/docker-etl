"""A Looker API client for the pipeline, with credentials from the system keyring.

The client id and secret are read through the keyring package, which uses the
macOS Keychain, Windows Credential Manager or Linux Secret Service. They are
stored under the services looker-api-client-id and looker-api-client-secret,
with the login name as the account:

    keyring set looker-api-client-id "$USER"
    keyring set looker-api-client-secret "$USER"

Where no keyring is available (CI, a headless machine), LOOKERSDK_CLIENT_ID and
LOOKERSDK_CLIENT_SECRET are read from the environment instead. Avoid exporting
those in a shell that runs an agent, since every command it runs inherits them.

The credentials are read once per process and handed to looker_sdk through a
settings object, so they stay out of this process's environment and its
children's. They are never printed or written to disk. Only read endpoints are
used by the pipeline.
"""

import functools
import getpass
import os

BASE_URL = "https://mozilla.cloud.looker.com"
TIMEOUT = 300

_SERVICES = {"client_id": "looker-api-client-id", "client_secret": "looker-api-client-secret"}


def _from_keyring(service):
    import keyring
    from keyring.errors import KeyringError

    try:
        return keyring.get_password(service, getpass.getuser())
    except KeyringError:
        # includes NoKeyringError, raised when no backend is installed
        return None


@functools.cache
def _credentials():
    creds = {}
    for key, service in _SERVICES.items():
        value = _from_keyring(service) or os.environ.get(f"LOOKERSDK_{key.upper()}")
        if not value:
            raise RuntimeError(
                f'No Looker {key}: set it with `keyring set {service} "$USER"`'
                f" or in LOOKERSDK_{key.upper()}"
            )
        creds[key] = value
    return creds


def sdk():
    import looker_sdk
    from looker_sdk.rtl import api_settings

    class Settings(api_settings.ApiSettings):
        # AuthSession calls read_config at each login rather than keeping the
        # secret on the settings object, so the credentials come from here.
        def read_config(self):
            return {
                "base_url": BASE_URL,
                "verify_ssl": "true",
                "timeout": str(TIMEOUT),
                **_credentials(),
            }

    return looker_sdk.init40(config_settings=Settings())
