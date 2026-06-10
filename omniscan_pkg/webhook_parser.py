def parse_webhook(data):
    """
    Parses and normalizes Sonarr/Radarr webhook payloads.
    Returns a dictionary with 'name', 'details', 'poster_url', and 'type'.
    """
    try:
        def get_poster(obj):
            # Check for standard 'images' array, fallback to 'poster' or 'fanart'
            if 'images' in obj and isinstance(obj['images'], list) and len(obj['images']) > 0:
                for img in obj['images']:
                    if img.get('coverType') == 'poster':
                        return img.get('url')
                return obj['images'][0].get('url')
            return obj.get('poster') or obj.get('fanart')

        if 'series' in data:  # Sonarr
            series = data['series'].get('title', 'Unknown Series')
            episodes = data.get('episodes', [])
            poster = get_poster(data['series'])
            
            if episodes:
                ep = episodes[0]
                details = f"S{ep.get('seasonNumber', 0):02d}E{ep.get('episodeNumber', 0):02d}"
                return {"name": series, "details": details, "poster_url": poster, "type": "TV"}
            return {"name": series, "details": "New Episode", "poster_url": poster, "type": "TV"}
        
        elif 'movie' in data:  # Radarr
            movie = data['movie'].get('title', 'Unknown Movie')
            year = data['movie'].get('year', '')
            poster = get_poster(data['movie'])
            return {"name": movie, "details": str(year), "poster_url": poster, "type": "Movie"}
            
    except Exception:
        pass
    return None
