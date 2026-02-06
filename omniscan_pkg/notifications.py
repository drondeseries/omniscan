import logging
import os
import requests
import json
from datetime import datetime
from discord import Embed, Color

logger = logging.getLogger(__name__)

def truncate_field_value(value, max_length=1024):
    """Truncate field value to Discord's limit of 1024 characters."""
    if len(value) <= max_length:
        return value
    return value[:max_length-3] + "..."

def format_file_list(files, max_items=10, prefix="• ", code_block=False, language=""):
    """Format a list of files into a string with truncation."""
    if not files:
        return "None"
    
    items = files[:max_items]
    formatted = "\n".join([f"{prefix}{f}" for f in items])
    
    if len(files) > max_items:
        formatted += f"\n...and {len(files) - max_items} more"
    
    if code_block:
        res = f"```{language}\n{formatted}\n```"
        return truncate_field_value(res, 1024)
    return truncate_field_value(formatted, 1024)

def get_embed_length(embed):
    """Calculate the total character count of an embed as Discord does."""
    length = len(embed.title or "") + len(embed.description or "")
    if embed.author:
        length += len(embed.author.name or "")
    if embed.footer:
        length += len(embed.footer.text or "")
    for field in embed.fields:
        length += len(field.name) + len(field.value)
    return length

def send_discord_webhook_sync(webhook_url, embed, config):
    """Send a Discord webhook message synchronously using requests."""
    try:
        payload = {
            "username": config.get('DISCORD_WEBHOOK_NAME', 'Omniscan'),
            "avatar_url": config.get('DISCORD_AVATAR_URL'),
            "embeds": []
        }

        # Check if embed exceeds Discord's character limit (6000)
        if get_embed_length(embed) > 6000:
            # Simple fallback: just send the base info
            base_embed = Embed(
                title=embed.title,
                description=embed.description,
                color=embed.color,
                timestamp=embed.timestamp
            )
            if embed.fields:
                # Add only the first field (usually overview)
                base_embed.add_field(name=embed.fields[0].name, value=embed.fields[0].value, inline=False)
            
            payload["embeds"].append(base_embed.to_dict())
            
            # Add a note about truncation
            payload["embeds"][0]["footer"] = {"text": "Note: Some details were truncated due to Discord length limits."}
        else:
            payload["embeds"].append(embed.to_dict())

        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Failed to send sync webhook: {str(e)}")
        return False
