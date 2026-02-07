import logging
import os
import requests
import json
from datetime import datetime
from discord import Embed, Color

logger = logging.getLogger(__name__)

def truncate_field_value(value, max_length=1024):
    """Truncate field value to Discord's limit of 1024 characters."""
    if value is None:
        return ""
    value = str(value)
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
        # Truncate content BEFORE wrapping in code block to ensure it's closed correctly
        # Discord's limit is 1024. Leave room for ```language\n and \n```
        max_inner = 1000 - len(language)
        formatted = truncate_field_value(formatted, max_inner)
        return f"```{language}\n{formatted}\n```"
        
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
    if not webhook_url or not str(webhook_url).startswith("http"):
        return False
        
    webhook_url = str(webhook_url).strip()
    
    try:
        payload = {
            "embeds": []
        }
        
        username = config.get('DISCORD_WEBHOOK_NAME')
        if username:
            payload["username"] = truncate_field_value(username, 80)
            
        avatar_url = config.get('DISCORD_AVATAR_URL')
        if avatar_url:
            payload["avatar_url"] = avatar_url

        # Ensure individual field limits are respected before sending
        if embed.title:
            embed.title = truncate_field_value(embed.title, 256)
        if embed.description:
            embed.description = truncate_field_value(embed.description, 4096)
        
        if embed.footer and embed.footer.text:
            embed.set_footer(text=truncate_field_value(embed.footer.text, 2048))
            
        if embed.author and embed.author.name:
            embed.set_author(name=truncate_field_value(embed.author.name, 256))

        for i, field in enumerate(embed.fields):
            embed.set_field_at(
                i, 
                name=truncate_field_value(field.name, 256), 
                value=truncate_field_value(field.value, 1024), 
                inline=field.inline
            )

        # Check if total embed length exceeds Discord's character limit (6000)
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
