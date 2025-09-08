def is_allowed(license_str: str, allowed: list[str]) -> bool:
    return license_str in set(allowed)
