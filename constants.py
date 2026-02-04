from __future__ import annotations

import re
from typing import Iterable

PLATFORMS = [
    ("youtube", "YouTube"),
    ("instagram", "Instagram"),
    ("tiktok", "TikTok"),
    ("linkedin", "LinkedIn"),
    ("twitter", "X/Twitter"),
    ("twitch", "Twitch"),
    ("podcast", "Podcast"),
    ("newsletter", "Newsletter"),
    ("other", "Other"),
]

NICHES = [
    ("finance", "Finance"),
    ("beauty", "Beauty"),
    ("fashion", "Fashion"),
    ("gaming", "Gaming"),
    ("tech", "Tech"),
    ("fitness", "Fitness"),
    ("health", "Health"),
    ("lifestyle", "Lifestyle"),
    ("travel", "Travel"),
    ("food", "Food"),
    ("parenting", "Parenting"),
    ("education", "Education"),
    ("business", "Business"),
    ("self_improvement", "Self-Improvement"),
    ("music", "Music"),
    ("sports", "Sports"),
    ("comedy", "Comedy"),
    ("other", "Other"),
]

GEO_REGIONS = [
    ("us", "US"),
    ("canada", "Canada"),
    ("uk", "UK"),
    ("eu", "Europe (EU)"),
    ("latam", "Latin America (LATAM)"),
    ("apac", "Asia-Pacific (APAC)"),
    ("other", "Other"),
]

FOLLOWER_TIERS = [
    ("under_5k", "Under 5K"),
    ("5k_10k", "5K–10K"),
    ("10k_25k", "10K–25K"),
    ("25k_50k", "25K–50K"),
    ("50k_100k", "50K–100K"),
    ("100k_plus", "100K+"),
]

DEAL_TYPES = [
    ("dedicated_video", "Dedicated Video"),
    ("integration", "Integration"),
    ("ugc_only", "UGC Only"),
    ("story_bundle", "Story Bundle"),
    ("feed_post", "Feed Post"),
    ("reel_short", "Reel/Short"),
    ("long_form", "Long Form"),
    ("newsletter_mention", "Newsletter Mention"),
    ("podcast_read", "Podcast Read"),
    ("other", "Other"),
]

CONTENT_FORMATS = [
    ("short_video", "Short Video"),
    ("long_video", "Long Video"),
    ("static_post", "Static Post"),
    ("story", "Story"),
    ("carousel", "Carousel"),
    ("newsletter", "Newsletter"),
    ("podcast", "Podcast"),
    ("other", "Other"),
]

PLATFORM_LABELS = dict(PLATFORMS)
NICHE_LABELS = dict(NICHES)
GEO_REGION_LABELS = dict(GEO_REGIONS)
FOLLOWER_TIER_LABELS = dict(FOLLOWER_TIERS)
DEAL_TYPE_LABELS = dict(DEAL_TYPES)
CONTENT_FORMAT_LABELS = dict(CONTENT_FORMATS)


def _slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return re.sub(r"_+", "_", value).strip("_")


def _build_label_lookup(options: Iterable[tuple[str, str]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for code, label in options:
        lookup[_slugify(label)] = code
        lookup[_slugify(code)] = code
    return lookup


_PLATFORM_ALIASES = {
    "x": "twitter",
    "twitter": "twitter",
    "x_twitter": "twitter",
    "yt": "youtube",
}

_GEO_ALIASES = {
    "usa": "us",
    "u_s": "us",
    "u_s_a": "us",
    "united_states": "us",
    "united_states_of_america": "us",
    "united_kingdom": "uk",
    "great_britain": "uk",
    "england": "uk",
    "europe": "eu",
    "europe_eu": "eu",
    "european_union": "eu",
    "latin_america": "latam",
    "south_america": "latam",
    "asia_pacific": "apac",
}

_FOLLOWER_TIER_ALIASES = {
    "under_5k": "under_5k",
    "under5k": "under_5k",
    "5k_10k": "5k_10k",
    "5k10k": "5k_10k",
    "10k_25k": "10k_25k",
    "10k25k": "10k_25k",
    "25k_50k": "25k_50k",
    "25k50k": "25k_50k",
    "50k_100k": "50k_100k",
    "50k100k": "50k_100k",
    "100k_plus": "100k_plus",
    "100k": "100k_plus",
    "100kplus": "100k_plus",
}

_PLATFORM_LOOKUP = _build_label_lookup(PLATFORMS)
_NICHE_LOOKUP = _build_label_lookup(NICHES)
_GEO_LOOKUP = _build_label_lookup(GEO_REGIONS)
_DEAL_TYPE_LOOKUP = _build_label_lookup(DEAL_TYPES)
_CONTENT_FORMAT_LOOKUP = _build_label_lookup(CONTENT_FORMATS)
_FOLLOWER_TIER_LOOKUP = _build_label_lookup(FOLLOWER_TIERS)


def normalize_choice(
    value: str | None,
    allowed: set[str],
    lookup: dict[str, str],
    aliases: dict[str, str] | None = None,
    default: str = "other",
) -> str:
    if value is None:
        return default
    raw = value.strip().lower()
    if raw == "":
        return default
    if raw in allowed:
        return raw
    if aliases and raw in aliases:
        return aliases[raw]
    slug = _slugify(raw)
    if slug in allowed:
        return slug
    if aliases and slug in aliases:
        return aliases[slug]
    if slug in lookup:
        return lookup[slug]
    return default


def normalize_platform(value: str | None) -> str:
    return normalize_choice(value, set(PLATFORM_LABELS.keys()), _PLATFORM_LOOKUP, _PLATFORM_ALIASES)


def normalize_niche(value: str | None) -> str:
    return normalize_choice(value, set(NICHE_LABELS.keys()), _NICHE_LOOKUP)


def normalize_geo_region(value: str | None) -> str:
    return normalize_choice(value, set(GEO_REGION_LABELS.keys()), _GEO_LOOKUP, _GEO_ALIASES)


def normalize_deal_type(value: str | None) -> str:
    return normalize_choice(value, set(DEAL_TYPE_LABELS.keys()), _DEAL_TYPE_LOOKUP)


def normalize_content_format(value: str | None) -> str:
    return normalize_choice(value, set(CONTENT_FORMAT_LABELS.keys()), _CONTENT_FORMAT_LOOKUP)


def normalize_follower_tier(value: str | None) -> str:
    return normalize_choice(value, set(FOLLOWER_TIER_LABELS.keys()), _FOLLOWER_TIER_LOOKUP, _FOLLOWER_TIER_ALIASES, default="under_5k")


def platform_label(value: str | None) -> str:
    return PLATFORM_LABELS.get(normalize_platform(value), "Other")


def niche_label(value: str | None) -> str:
    return NICHE_LABELS.get(normalize_niche(value), "Other")


def geo_region_label(value: str | None) -> str:
    return GEO_REGION_LABELS.get(normalize_geo_region(value), "Other")


def follower_tier_label(value: str | None) -> str:
    return FOLLOWER_TIER_LABELS.get(normalize_follower_tier(value), "Under 5K")


def deal_type_label(value: str | None) -> str:
    return DEAL_TYPE_LABELS.get(normalize_deal_type(value), "Other")


def content_format_label(value: str | None) -> str:
    return CONTENT_FORMAT_LABELS.get(normalize_content_format(value), "Other")
