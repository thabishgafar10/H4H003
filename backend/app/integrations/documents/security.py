"""
Document Ingestion Security:
- SSRF prevention & Private IP / Loopback / Cloud metadata blocking
- Upload file type & size limit validation
- Path traversal sanitization
"""

import ipaddress
import os
import re
import socket
from urllib.parse import urlparse

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".txt", ".md"}
MAX_FILE_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB limit


def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and arbitrary filesystem writes."""
    basename = os.path.basename(filename.replace("\\", "/"))
    # Remove null bytes and non-printable chars
    clean = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", basename)
    # Strip dangerous leading/trailing characters
    clean = clean.strip(". ")
    if not clean:
        clean = "document.txt"
    return clean


def validate_file_upload(filename: str, file_size: int) -> None:
    """Validate uploaded document file extension and file size."""
    ext = os.path.splitext(filename.lower())[1]
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file format '{ext}'. Allowed formats are: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )
    if file_size > MAX_FILE_SIZE_BYTES:
        raise ValueError(
            f"File size ({file_size} bytes) exceeds maximum allowed limit of {MAX_FILE_SIZE_BYTES} bytes (10MB)."
        )
    if file_size <= 0:
        raise ValueError("File is empty (0 bytes).")


def is_ip_private_or_restricted(ip_str: str) -> bool:
    """Check if an IP address is loopback, RFC1918 private, link-local, multicast, or cloud metadata."""
    try:
        ip = ipaddress.ip_address(ip_str)
        if (
            ip.is_private
            or ip.is_loopback
            or ip.is_link_local
            or ip.is_reserved
            or ip.is_multicast
            or ip.is_unspecified
        ):
            return True
        # Explicit cloud metadata check (e.g. AWS/GCP/Azure link-local metadata 169.254.169.254)
        if str(ip) == "169.254.169.254":
            return True
        return False
    except ValueError:
        return True


def validate_public_url(url: str) -> None:
    """
    Validate that a URL is a legitimate public web URL and does not target
    loopback, private internal network ranges, or cloud metadata endpoints.
    """
    if not url:
        raise ValueError("URL cannot be empty.")

    parsed = urlparse(url)
    if parsed.scheme.lower() not in ("http", "https"):
        raise ValueError(f"Invalid URL scheme '{parsed.scheme}'. Only http and https are allowed.")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL must have a valid hostname.")

    # Check for localhost / loopback aliases directly
    lower_host = hostname.lower()
    if lower_host in ("localhost", "127.0.0.1", "::1", "0.0.0.0", "metadata.google.internal"):
        raise ValueError(f"Access to restricted hostname '{hostname}' is forbidden.")

    # Resolve hostname to all associated IP addresses
    try:
        addr_info = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise ValueError(f"Unable to resolve hostname '{hostname}': {e}")

    for entry in addr_info:
        sockaddr = entry[4]
        ip_str = sockaddr[0]
        if is_ip_private_or_restricted(ip_str):
            raise ValueError(
                f"Security violation: Resolved IP address '{ip_str}' for host '{hostname}' "
                f"is a private or restricted address (SSRF protection)."
            )
