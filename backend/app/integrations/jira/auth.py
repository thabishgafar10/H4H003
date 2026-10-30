"""
Jira Cloud Authentication Support:
- HTTP Basic Auth with API Token (email + api_token)
- OAuth 2.0 3LO (Authorization Code flow & Refresh Token flow)
"""

import base64
from typing import Dict, Any, Optional
import httpx


class JiraAuthHandler:
    @staticmethod
    def get_basic_auth_header(email: str, api_token: str) -> Dict[str, str]:
        token_str = f"{email}:{api_token}"
        encoded = base64.b64encode(token_str.encode("utf-8")).decode("utf-8")
        return {"Authorization": f"Basic {encoded}"}

    @staticmethod
    def get_oauth_header(access_token: str) -> Dict[str, str]:
        return {"Authorization": f"Bearer {access_token}"}

    @staticmethod
    async def exchange_oauth_code(
        client_id: str,
        client_secret: str,
        code: str,
        redirect_uri: str,
    ) -> Dict[str, Any]:
        """Exchange OAuth authorization code for tokens."""
        url = "https://auth.atlassian.com/oauth/token"
        payload = {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    async def refresh_oauth_token(
        client_id: str,
        client_secret: str,
        refresh_token: str,
    ) -> Dict[str, Any]:
        """Refresh an expired OAuth access token."""
        url = "https://auth.atlassian.com/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            return resp.json()
