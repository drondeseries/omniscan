import logging
import os
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

async def send_discord_webhook(webhook, embed, config):
    """Send a Discord webhook message."""
    try:
        # Check if embed exceeds Discord's limits
        if get_embed_length(embed) > 6000:
            # Split into multiple embeds
            base_embed = Embed(
                title=embed.title,
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add overview field
            if embed.fields and embed.fields[0].name == "📊 Overview":
                base_embed.add_field(
                    name=embed.fields[0].name,
                    value=embed.fields[0].value,
                    inline=False
                )
            
            # Send base embed
            await webhook.send(
                embed=base_embed,
                avatar_url=config['DISCORD_AVATAR_URL'],
                username=config['DISCORD_WEBHOOK_NAME'],
                wait=True
            )
            
            # Create additional embeds for libraries
            current_embed = Embed(
                title="📁 Library Details",
                color=embed.color,
                timestamp=embed.timestamp
            )
            
            # Add library fields
            for field in embed.fields[1:]:
                if field.name.startswith("📁"):
                    if len(str(current_embed)) + len(str(field)) > 6000:
                        # Send current embed and create new one
                        await webhook.send(
                            embed=current_embed,
                            avatar_url=config['DISCORD_AVATAR_URL'],
                            username=config['DISCORD_WEBHOOK_NAME'],
                            wait=True
                        )
                        current_embed = Embed(
                            title="📁 Library Details (continued)",
                            color=embed.color,
                            timestamp=embed.timestamp
                        )
                    current_embed.add_field(
                        name=field.name,
                        value=field.value,
                        inline=field.inline
                    )
            
            # Send final library embed if it has fields
            if current_embed.fields:
                await webhook.send(
                    embed=current_embed,
                    avatar_url=config['DISCORD_AVATAR_URL'],
                    username=config['DISCORD_WEBHOOK_NAME'],
                    wait=True
                )
            
            # Send issues in separate embed if they exist
            if embed.fields and embed.fields[-1].name == "⚠️ Issues":
                issues_embed = Embed(
                    title="⚠️ Issues",
                    color=Color.red(),
                    timestamp=embed.timestamp
                )
                issues_embed.add_field(
                    name=embed.fields[-1].name,
                    value=embed.fields[-1].value,
                    inline=False
                )
                await webhook.send(
                    embed=issues_embed,
                    avatar_url=config['DISCORD_AVATAR_URL'],
                    username=config['DISCORD_WEBHOOK_NAME'],
                    wait=True
                )
        else:
            # Send single embed if within limits
            await webhook.send(
                embed=embed,
                avatar_url=config['DISCORD_AVATAR_URL'],
                username=config['DISCORD_WEBHOOK_NAME'],
                wait=True
            )
    except Exception as e:
        logger.error(f"Failed to send webhook: {str(e)}")
        raise
