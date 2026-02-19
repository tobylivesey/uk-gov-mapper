"""
Mail provider detection from MX records.

Parses MX hostnames to identify the underlying mail provider/service.
Returns structured data instead of relying on fallback "Other" values.
"""

import re
from dataclasses import dataclass
from typing import Optional


@dataclass
class MailProviderMatch:
    """Result of mail provider detection."""
    provider: str
    category: str  # e.g., "cloud", "security_gateway", "self_hosted", "isp"
    confidence: str  # "high", "medium", "low"
    matched_pattern: str  # The pattern that matched


# Patterns ordered by specificity - more specific patterns first
# Each tuple: (pattern, provider_name, category, confidence)
MX_PATTERNS: list[tuple[str, str, str, str]] = [
    # Cloud email providers (high confidence)
    (r"google\.com$|googlemail\.com$|aspmx\.l\.google\.com$", "Google Workspace", "cloud", "high"),
    (r"outlook\.com$|mail\.protection\.outlook\.com$", "Microsoft 365", "cloud", "high"),
    (r"pphosted\.com$|proofpoint\.com$|ppe-hosted\.com$", "Proofpoint", "security_gateway", "high"),
    (r"mimecast\.com$|mimecast\.co\.uk$", "Mimecast", "security_gateway", "high"),
    (r"messagelabs\.com$|symantec.*cloud|brightmail", "Symantec/Broadcom", "security_gateway", "high"),
    (r"barracuda\.com$|barracudanetworks\.com$|cuda-inc\.com$", "Barracuda", "security_gateway", "high"),
    (r"sophos\.com$|reflexion\.net$", "Sophos", "security_gateway", "high"),
    (r"forcepoint\.com$|websense\.com$|mailcontrol\.com$", "Forcepoint", "security_gateway", "high"),
    (r"iphmx\.com$|trendmicro\.com$|tmes\.trendmicro\.com$", "Trend Micro", "security_gateway", "high"),
    (r"fortimail\.com$|fortinet\.net$", "Fortinet FortiMail", "security_gateway", "high"),
    (r"titanhq\.com$|spamtitan\.com$", "TitanHQ SpamTitan", "security_gateway", "high"),
    (r"mailanyone\.net$", "MailAnyone", "security_gateway", "high"),
    (r"bangdynamics\.com$", "BangDynamics", "security_gateway", "medium"),

    # Cloud infrastructure
    (r"amazonaws\.com$|amazonses\.com$|inbound-smtp.*\.amazonaws\.com$", "Amazon SES", "cloud", "high"),
    (r"sendgrid\.net$", "SendGrid", "cloud", "high"),
    (r"mailgun\.org$", "Mailgun", "cloud", "high"),
    (r"mandrillapp\.com$|mandrill\.com$", "Mailchimp Mandrill", "cloud", "high"),
    (r"postmarkapp\.com$", "Postmark", "cloud", "high"),
    (r"sparkpostmail\.com$", "SparkPost", "cloud", "high"),
    (r"zoho\.com$|zoho\.eu$|zohomail\.com$", "Zoho Mail", "cloud", "high"),
    (r"fastmail\.com$|messagingengine\.com$", "Fastmail", "cloud", "high"),
    (r"tutanota\.de$|tutamail\.com$", "Tutanota", "cloud", "high"),
    (r"protonmail\.ch$|proton\.me$", "ProtonMail", "cloud", "high"),
    (r"mailchimp\.com$|mcsv\.net$", "Mailchimp", "cloud", "high"),

    # UK Government specific
    (r"gsi\.gov\.uk$", "GSI (Government Secure Intranet)", "government", "high"),
    (r"gcsx\.gov\.uk$", "GCSX", "government", "high"),
    (r"defencegateway\.mod\.uk$", "Defence Gateway", "government", "high"),
    (r"gcbps\.mod\.uk$", "MOD Global Communications", "government", "high"),
    (r"mod\.uk$", "Ministry of Defence", "government", "medium"),
    (r"nhs\.net$|nhs\.uk$", "NHS Mail", "government", "high"),
    (r"police\.uk$", "Police Service", "government", "medium"),

    # UK ISPs and hosting
    (r"secureserver\.net$", "GoDaddy", "isp", "high"),
    (r"123-reg\.co\.uk$|123-reg\.com$", "123-Reg", "isp", "high"),
    (r"livemail\.co\.uk$|livemail\.uk$", "Livemail UK", "isp", "high"),
    (r"names\.co\.uk$|nameservers\.com$", "Namesco", "isp", "high"),
    (r"ionos\.co\.uk$|1and1\.co\.uk$|perfora\.net$", "IONOS", "isp", "high"),
    (r"btconnect\.com$|btinternet\.com$", "BT", "isp", "high"),
    (r"claranet\.co\.uk$", "Claranet", "isp", "high"),
    (r"virginmedia\.com$|ntlworld\.com$", "Virgin Media", "isp", "high"),
    (r"fasthosts\.co\.uk$", "Fasthosts", "isp", "high"),
    (r"heartinternet\.uk$", "Heart Internet", "isp", "high"),
    (r"krystal\.io$|krystal\.co\.uk$", "Krystal Hosting", "isp", "high"),
    (r"tsohost\.co\.uk$", "Tsohost", "isp", "high"),
    (r"rackspace\.com$", "Rackspace", "cloud", "high"),
    (r"ovh\.net$|ovh\.co\.uk$", "OVH", "isp", "high"),

    # Email security gateways
    (r"scanscope\.net$", "Scanscope", "security_gateway", "high"),
    (r"topsec\.com$", "TopSec", "security_gateway", "high"),
    (r"emailsecurity\.app$", "Email Security App", "security_gateway", "medium"),
    (r"mx25\.net$", "MX25", "security_gateway", "medium"),
    (r"ess\.barracudanetworks\.com$", "Barracuda ESS", "security_gateway", "high"),
    (r"spamh\.com$|antispam.*\.com$", "Anti-Spam Service", "security_gateway", "medium"),
    (r"spamexperts\.com$|spamexperts\.net$", "SpamExperts", "security_gateway", "high"),
    (r"fusemail\.net$|fusemail\.com$", "Fusemail", "security_gateway", "high"),
    (r"appriver\.com$", "AppRiver", "security_gateway", "high"),
    (r"hornetsecurity\.com$", "Hornetsecurity", "security_gateway", "high"),
    (r"mailroute\.net$", "MailRoute", "security_gateway", "high"),
    (r"zix\.com$|zixmail\.net$", "Zix", "security_gateway", "high"),

    # Generic gov.uk domains (lower confidence - could be self-hosted)
    (r"\.gov\.uk$", "gov.uk domain", "government", "low"),
    (r"\.ac\.uk$", "UK Academic", "academic", "low"),
    (r"\.nhs\.uk$", "NHS domain", "government", "low"),
]


def parse_mx_provider(mx_host: str) -> Optional[MailProviderMatch]:
    """
    Parse an MX hostname and identify the mail provider.

    Args:
        mx_host: The MX record hostname (e.g., "example-gov-uk.mail.protection.outlook.com")

    Returns:
        MailProviderMatch if a provider is identified, None otherwise
    """
    if not mx_host:
        return None

    mx_lower = mx_host.lower().strip().rstrip(".")

    for pattern, provider, category, confidence in MX_PATTERNS:
        if re.search(pattern, mx_lower):
            return MailProviderMatch(
                provider=provider,
                category=category,
                confidence=confidence,
                matched_pattern=pattern
            )

    return None


def get_mail_provider(mx_records: list[dict]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Determine the mail provider from a list of MX records.

    Examines all MX records (not just primary) to better identify the underlying provider,
    since security gateways often front the actual mail service.

    Args:
        mx_records: List of dicts with 'host' and 'priority' keys

    Returns:
        Tuple of (provider_name, category, confidence)
        Returns (None, None, None) if no records or no match
    """
    if not mx_records:
        return None, None, None

    # First, try the primary (lowest priority number) MX record
    primary_host = mx_records[0].get("host", "")
    primary_match = parse_mx_provider(primary_host)

    if primary_match and primary_match.confidence == "high":
        return primary_match.provider, primary_match.category, primary_match.confidence

    # If primary is a security gateway, check if we can find the actual mail provider
    # by looking at secondary MX records
    all_matches = []
    for mx in mx_records:
        match = parse_mx_provider(mx.get("host", ""))
        if match:
            all_matches.append(match)

    # Prefer cloud providers over security gateways when both exist
    cloud_matches = [m for m in all_matches if m.category == "cloud"]
    if cloud_matches:
        best = max(cloud_matches, key=lambda m: ("high", "medium", "low").index(m.confidence) if m.confidence in ("high", "medium", "low") else 3)
        return best.provider, best.category, best.confidence

    # Otherwise return the best match we have
    if all_matches:
        # Sort by confidence (high > medium > low)
        confidence_order = {"high": 0, "medium": 1, "low": 2}
        best = min(all_matches, key=lambda m: confidence_order.get(m.confidence, 3))
        return best.provider, best.category, best.confidence

    # Check if it looks like self-hosted (mail.domain.tld pattern)
    if primary_host and re.match(r"^mail\d*\.", primary_host.lower()):
        return "Self-hosted", "self_hosted", "medium"

    # If we have MX records but couldn't identify the provider
    if primary_host:
        return f"Unknown ({_extract_base_domain(primary_host)})", "unknown", "low"

    return None, None, None


def _extract_base_domain(hostname: str) -> str:
    """Extract a meaningful identifier from a hostname."""
    parts = hostname.lower().rstrip(".").split(".")
    if len(parts) >= 2:
        # Return last two parts (e.g., "example.com")
        return ".".join(parts[-2:])
    return hostname


def get_primary_mail_provider(mx_records: list[dict]) -> Optional[str]:
    """
    Legacy function signature for backwards compatibility.
    Returns just the provider name.
    """
    provider, _, _ = get_mail_provider(mx_records)
    return provider
