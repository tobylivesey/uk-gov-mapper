"""
Cybersecurity technology taxonomy for vendor/product extraction from job descriptions.

Categories aligned with SOC-CMM Technology domain (visibility triad + SOAR)
plus adjacent security controls (Firewall, IAM, Vulnerability Management).

Each category maps vendor display names to a list of keyword patterns.
Patterns are matched case-insensitively against job description text.
Longer/more specific patterns are listed first to allow greedy matching.
"""

TAXONOMY: dict[str, dict[str, list[str]]] = {
    # --- SOC-CMM Visibility Triad ---

    "siem": {
        "Splunk": [
            "splunk enterprise security",
            "splunk es",
            "splunk cloud",
            "splunk enterprise",
            "splunk siem",
            "splunk",
        ],
        "Microsoft Sentinel": [
            "microsoft sentinel",
            "azure sentinel",
            "ms sentinel",
            "sentinel siem",
        ],
        "IBM QRadar": [
            "ibm qradar",
            "qradar",
        ],
        "Elastic Security": [
            "elastic",
            "elastic siem",
            "elastic security",
            "elasticsearch security",
            "elk siem",
            "elk stack",
        ],
        "Google Chronicle": [
            "google chronicle",
            "chronicle siem",
            "chronicle security",
        ],
        "LogRhythm": [
            "logrhythm",
            "log rhythm",
        ],
        "ArcSight": [
            "micro focus arcsight",
            "arcsight",
        ],
        "Exabeam": [
            "exabeam",
        ],
        "Securonix": [
            "securonix",
        ],
        "Sumo Logic": [
            "sumo logic",
        ],
        "AlienVault / AT&T USM": [
            "alienvault",
            "alien vault",
            "att usm",
            "at&t usm",
        ],
        "Graylog": [
            "graylog",
        ],
        "Wazuh": [
            "wazuh",
        ],
    },

    "edr": {
        "CrowdStrike Falcon": [
            "crowdstrike falcon",
            "crowd strike falcon",
            "crowdstrike edr",
            "crowdstrike",
            "crowd strike",
        ],
        "Microsoft Defender for Endpoint": [
            "microsoft defender for endpoint",
            "defender for endpoint",
            "ms defender for endpoint",
            "microsoft defender atp",
            "defender atp",
            "mde ",  # trailing space to avoid false positives
        ],
        "SentinelOne": [
            "sentinelone",
            "sentinel one",
            "s1 edr",
        ],
        "Carbon Black": [
            "vmware carbon black",
            "carbon black",
            "cb defense",
            "cb defence",
        ],
        "Tanium": [
            "tanium",
        ],
        "Cortex XDR": [
            "palo alto cortex xdr",
            "cortex xdr",
        ],
        "Trend Micro Vision One": [
            "trend micro vision one",
            "vision one",
            "trend micro xdr",
            "trend micro apex one",
            "apex one",
        ],
        "Sophos Intercept X": [
            "sophos intercept x",
            "intercept x",
            "sophos edr",
            "sophos xdr",
        ],
        "Cybereason": [
            "cybereason",
        ],
        "Trellix EDR": [
            "trellix edr",
            "trellix endpoint",
            "fireeye endpoint",
            "mcafee edr",
        ],
        "Cisco Secure Endpoint": [
            "cisco secure endpoint",
            "cisco amp for endpoints",
            "amp for endpoints",
        ],
    },

    "ndr": {
        "Darktrace": [
            "darktrace",
            "dark trace",
        ],
        "Vectra AI": [
            "vectra ai",
            "vectra ndr",
            "vectra",
        ],
        "Corelight": [
            "corelight",
        ],
        "ExtraHop Reveal(x)": [
            "extrahop reveal",
            "extrahop",
            "reveal(x)",
        ],
        "Cisco Stealthwatch": [
            "cisco stealthwatch",
            "stealthwatch",
            "cisco secure network analytics",
        ],
        "Zeek": [
            "zeek",
            "bro ids",
        ],
        "Snort": [
            "snort ids",
            "snort ips",
            "snort",
        ],
        "Suricata": [
            "suricata",
        ],
        "Gigamon": [
            "gigamon",
        ],
    },

    # --- SOC-CMM Automation ---

    "soar": {
        "Palo Alto XSOAR": [
            "palo alto xsoar",
            "cortex xsoar",
            "demisto",
            "xsoar",
        ],
        "Splunk SOAR": [
            "splunk soar",
            "splunk phantom",
            "phantom soar",
        ],
        "Swimlane": [
            "swimlane",
        ],
        "TheHive": [
            "thehive",
            "the hive",
        ],
        "ServiceNow SecOps": [
            "servicenow security operations",
            "servicenow secops",
            "servicenow sir",
        ],
        "IBM Resilient": [
            "ibm resilient",
            "ibm soar",
            "resilient soar",
        ],
        "Tines": [
            "tines soar",
            "tines automation",
            "tines",
        ],
        "Microsoft Sentinel SOAR": [
            "sentinel soar",
            "logic apps security",
        ],
        "Siemplify": [
            "siemplify",
            "google soar",
        ],
    },

    # --- Adjacent Security Controls ---

    "firewall": {
        "Palo Alto Networks": [
            "palo alto networks firewall",
            "palo alto firewall",
            "palo alto ngfw",
            "pan-os",
            "panos",
            "pa-series",
        ],
        "Fortinet FortiGate": [
            "fortinet fortigate",
            "fortigate",
            "fortios",
            "fortinet firewall",
        ],
        "Check Point": [
            "check point firewall",
            "check point ngfw",
            "checkpoint firewall",
            "check point",
            "checkpoint",
        ],
        "Cisco Firepower": [
            "cisco firepower",
            "firepower threat defense",
            "cisco asa",
            "cisco ftd",
        ],
        "Sophos XG": [
            "sophos xg firewall",
            "sophos xgs",
            "sophos firewall",
            "sophos xg",
        ],
        "Juniper SRX": [
            "juniper srx",
            "junos firewall",
            "juniper firewall",
        ],
        "pfSense": [
            "pfsense",
        ],
        "WatchGuard": [
            "watchguard firebox",
            "watchguard firewall",
            "watchguard",
        ],
        "Barracuda CloudGen": [
            "barracuda cloudgen firewall",
            "barracuda firewall",
        ],
    },

    "iam": {
        "Microsoft Entra ID": [
            "microsoft entra id",
            "entra id",
            "azure active directory",
            "azure ad",
        ],
        "CyberArk": [
            "cyberark pam",
            "cyberark privileged",
            "cyberark",
        ],
        "BeyondTrust": [
            "beyondtrust",
            "beyond trust",
        ],
        "Okta": [
            "okta",
        ],
        "SailPoint": [
            "sailpoint",
            "sail point",
        ],
        "Ping Identity": [
            "ping identity",
            "pingidentity",
            "pingfederate",
            "pingone",
        ],
        "Thales SafeNet": [
            "thales safenet",
            "safenet trusted access",
        ],
        "ForgeRock": [
            "forgerock",
        ],
        "HashiCorp Vault": [
            "hashicorp vault",
            "vault secrets",
        ],
        "AWS IAM": [
            "aws iam",
            "aws identity",
        ],
    },

    "vulnerability_management": {
        "Qualys": [
            "qualys vmdr",
            "qualys vulnerability",
            "qualys guard",
            "qualys",
        ],
        "Tenable": [
            "tenable one",
            "tenable io",
            "tenable.io",
            "tenable.sc",
            "tenable nessus",
            "nessus",
            "tenable",
        ],
        "Rapid7 InsightVM": [
            "rapid7 insightvm",
            "insightvm",
            "rapid7 nexpose",
            "nexpose",
            "rapid7",
        ],
        "Microsoft Defender Vulnerability Management": [
            "defender vulnerability management",
            "microsoft defender vulnerability",
        ],
        "CrowdStrike Spotlight": [
            "crowdstrike spotlight",
        ],
        "Tanium Comply": [
            "tanium comply",
        ],
        "OpenVAS": [
            "openvas",
            "greenbone",
        ],
    },
}


# Flat list of all category names for iteration
CATEGORIES = list(TAXONOMY.keys())
