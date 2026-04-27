# 26 Australian cities with coordinates and timezones
LOCATIONS = {
    "Albury": {"lat": -36.08, "lon": 146.91, "timezone": "Australia/Sydney", "state": "NSW"},
    "Ballarat": {"lat": -37.56, "lon": 143.85, "timezone": "Australia/Melbourne", "state": "VIC"},
    "Bendigo": {"lat": -36.76, "lon": 144.28, "timezone": "Australia/Melbourne", "state": "VIC"},
    "Brisbane": {"lat": -27.47, "lon": 153.02, "timezone": "Australia/Brisbane", "state": "QLD"},
    "Cairns": {"lat": -16.92, "lon": 145.77, "timezone": "Australia/Brisbane", "state": "QLD"},
    "Canberra": {"lat": -35.28, "lon": 149.13, "timezone": "Australia/Sydney", "state": "ACT"},
    "Darwin": {"lat": -12.46, "lon": 130.84, "timezone": "Australia/Darwin", "state": "NT"},
    "GoldCoast": {"lat": -28.02, "lon": 153.40, "timezone": "Australia/Brisbane", "state": "QLD"},
    "Hobart": {"lat": -42.88, "lon": 147.33, "timezone": "Australia/Hobart", "state": "TAS"},
    "Katherine": {"lat": -14.47, "lon": 132.27, "timezone": "Australia/Darwin", "state": "NT"},
    "Launceston": {"lat": -41.43, "lon": 147.14, "timezone": "Australia/Hobart", "state": "TAS"},
    "Melbourne": {"lat": -37.81, "lon": 144.96, "timezone": "Australia/Melbourne", "state": "VIC"},
    "Mildura": {"lat": -34.18, "lon": 142.16, "timezone": "Australia/Melbourne", "state": "VIC"},
    "MountGambier": {"lat": -37.83, "lon": 140.78, "timezone": "Australia/Adelaide", "state": "SA"},
    "Newcastle": {"lat": -32.93, "lon": 151.78, "timezone": "Australia/Sydney", "state": "NSW"},
    "Nuriootpa": {"lat": -34.47, "lon": 138.99, "timezone": "Australia/Adelaide", "state": "SA"},
    "Perth": {"lat": -31.95, "lon": 115.86, "timezone": "Australia/Perth", "state": "WA"},
    "Adelaide": {"lat": -34.93, "lon": 138.60, "timezone": "Australia/Adelaide", "state": "SA"},
    "AliceSprings": {"lat": -23.70, "lon": 133.88, "timezone": "Australia/Darwin", "state": "NT"},
    "Sydney": {"lat": -33.87, "lon": 151.21, "timezone": "Australia/Sydney", "state": "NSW"},
    "Townsville": {"lat": -19.26, "lon": 146.82, "timezone": "Australia/Brisbane", "state": "QLD"},
    "Tuggeranong": {"lat": -35.42, "lon": 149.09, "timezone": "Australia/Sydney", "state": "ACT"},
    "WaggaWagga": {"lat": -35.12, "lon": 147.37, "timezone": "Australia/Sydney", "state": "NSW"},
    "Albany": {"lat": -35.02, "lon": 117.88, "timezone": "Australia/Perth", "state": "WA"},
    "Wollongong": {"lat": -34.43, "lon": 150.89, "timezone": "Australia/Sydney", "state": "NSW"},
    "Woomera": {"lat": -31.15, "lon": 136.82, "timezone": "Australia/Adelaide", "state": "SA"},
}


def get_location(city: str) -> dict:
    if city not in LOCATIONS:
        raise ValueError(f"Unknown city: {city}. Available: {list(LOCATIONS.keys())}")
    return LOCATIONS[city]
