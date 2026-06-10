def parse_webhook(data):
    """
    Parses and normalizes Sonarr/Radarr webhook payloads.
    Returns a dictionary with 'name' and 'details' or None.
    """
    try:
        if 'series' in data:  # Sonarr
            series = data['series'].get('title', 'Unknown Series')
            episodes = data.get('episodes', [])
            if episodes:
                ep = episodes[0]
                details = f"S{ep.get('seasonNumber', 0):02d}E{ep.get('episodeNumber', 0):02d}"
                return {"name": series, "details": details}
            return {"name": series, "details": "New Episode"}
        
        elif 'movie' in data:  # Radarr
            movie = data['movie'].get('title', 'Unknown Movie')
            year = data['movie'].get('year', '')
            return {"name": movie, "details": str(year)}
            
    except Exception:
        pass
    return None
