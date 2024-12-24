import asyncio
from datetime import datetime
import re
import discord
import configparser
import aiohttp
import time
import requests

# Constants
check_feed_delay = 60
feed_url = "https://api.open511.gov.bc.ca/events?area_id=drivebc.ca/2"

# Begin and read config file
config = configparser.ConfigParser()
config.read('env.ini')
discord_webhook_url = config['Constants']['discord_webhook_url']
discord_webhook_log_url = config['Constants']['discord_webhook_log_url']

config = configparser.ConfigParser()

async def start():
    try:
        api = requests.get(feed_url)
    except ConnectionError:
        await send_log("Unable to connect to api")
        return

    print(api.status_code)
    if api.status_code != 200:
        await send_log(f"API Response Code {api.status_code}")
        return

    parsed_api = api.json()

    config.read('history.ini')
    incidents = config.items('Incidents')
    incidents2 = []

    for incident in incidents:
        incidents2.append(incident[0])

    for event in parsed_api['events']:
        config.read('history.ini')

        event_id = event['id']
        event_updated = event['updated']

        try:
            event_last_updated = config.get('Last Updated', event_id)
            if event_last_updated != event_updated:
                config.set('Last Updated', event_id, event_updated)
                with open('history.ini', 'w') as configfile:
                    config.write(configfile)
                await check_if_should_be_notified(event=event, title_prefix="Updated")
        except configparser.NoOptionError:
            config.set('Last Updated', event_id, event_updated)
            with open('history.ini', 'w') as configfile:
                config.write(configfile)
            await check_if_should_be_notified(event=event, title_prefix="New")
            if event['headline'] == 'INCIDENT':
                incidents.append(event_id)

        if event['headline'] == 'INCIDENT':
            try:
                incidents2.remove(event_id.lower())
            except ValueError:
                pass
            config.set('Incidents', event_id, "0")
            with open('history.ini', 'w') as configfile:
                config.write(configfile)

    config.read('history.ini')
    for incident in incidents2:
        await send_webhook_removed(incident)
        config.remove_option('Incidents', incident)
        with open('history.ini', 'w') as configfile:
            config.write(configfile)

async def check_if_should_be_notified(event, title_prefix):
    if event['headline'] == 'INCIDENT':
        await send_webhook(trigger="Incident", event=event, title_prefix=title_prefix)
    elif "Closure" in event['description'] or 'closure' in event['description'] or 'closed' in event['description']:
        await send_webhook(trigger="Closure Involved", event=event, title_prefix=title_prefix)

async def send_webhook(trigger, event, title_prefix):
    event_short_id = event['id'].split('/')[1]

    # Get "Last Updated" and "Next Update" timestamps in unix style from event
    unix_timestamps = await get_unix_timestamps_from_event(event)

    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=f"{title_prefix} DriveBC Event")
        embed.add_field(name="ID", value=event_short_id.upper())
        embed.add_field(name="Triggered By", value=trigger)
        embed.add_field(name="Road", value=event['roads'][0]['name'])
        embed.add_field(name="Direction", value=event['roads'][0]['direction'])
        embed.add_field(name="Last Updated", value=f"<t:{unix_timestamps[1]}:R>")

        if unix_timestamps[0] is None:
            embed.add_field(name="Next Update", value=f"N/A")
        else:
            embed.add_field(name="Next Update", value=f"<t:{unix_timestamps[0]}:R>")

        embed.add_field(name="Links", value=f"https://beta.drivebc.ca/?type=event&id={event_short_id}")
        webhook = discord.Webhook.from_url(discord_webhook_url, session=session)
        await webhook.send(embed=embed)

async def send_webhook_removed(event_id):
    event_short_id = event_id.split('/')[1]
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=f"Removed DriveBC Event")
        embed.add_field(name="ID", value=event_short_id.upper())
        webhook = discord.Webhook.from_url(discord_webhook_url, session=session)
        await webhook.send(embed=embed)

# Returns None when a timestamp could not be parsed
async def get_unix_timestamps_from_event(event):
    event_description = event.get('description', '')
    event_last_updated_timestamp = event.get('updated', '')
    event_last_update_unix = await get_unix_timestamp_from_timestamp(event_last_updated_timestamp)

    event_description_split = event_description.split('.') # Split description into sentences. 3rd last contains the "next update" information.
    if len(event_description_split) < 3: # If description is not in the usual format. To prevent a ValueError down the road.
        return [None, event_last_update_unix]
    event_next_update_description = event_description_split[-3]

    event_next_update_unix = await get_unix_timestamp_from_description(event_next_update_description)
    return [event_next_update_unix, event_last_update_unix]

# Returns None when the description could not be parsed into a unix timestamp
async def get_unix_timestamp_from_description(description):
    # Split words in description to find the description format (If it includes a year or something)
    event_description_split = description.split(' ')

    if event_description_split[-1] != "PST": # If description does not include a timestamp
        return None

    if len(event_description_split[-5]) == 4 and event_description_split[-5].isnumeric(): # If description includes a year
        # Search if description contains this pattern
        pattern = r"(\w{3}) (\w{3}) (\d{1,2}), (\d{4}) at (\d{1,2}:\d{2} [APM]{2}) PST$"
        match = re.search(pattern, description)
        if not match: # If pattern could not be matched
            return None

        # Assign each time property (day, month, year, etc.) to a datetime object
        day_of_week, month, day, year, time_part = match.groups()
        timestamp_str = f"{day_of_week} {month} {day}, {year} at {time_part}"
        datetime_obj = datetime.strptime(timestamp_str, "%a %b %d, %Y at %I:%M %p")
        return int(datetime_obj.timestamp()) # Return unix value of the datetime object

    elif len(event_description_split[-5]) == 1 or len(event_description_split[-5]) == 2 and event_description_split[-5].isnumeric(): # If description does not contain a year
        pattern = r"(\w{3}) (\w{3}) (\d{1,2}) at (\d{1,2}:\d{2} [APM]{2}) PST$"
        match = re.search(pattern, description)
        if not match:
            return None
        day_of_week, month, day, time_part = match.groups()
        current_year = datetime.now().year
        timestamp_str = f"{day_of_week} {month} {day}, {current_year} at {time_part}"
        datetime_obj = datetime.strptime(timestamp_str, "%a %b %d, %Y at %I:%M %p")
        return int(datetime_obj.timestamp())

    else:
        return None

# Get unix timestamp from a timestamp like: 2024-12-19T13:40:20-08:00
async def get_unix_timestamp_from_timestamp(timestamp):
    # Remove the time zone offset (-08:00, or -07:00) from the string
    cleaned_timestamp = timestamp.rsplit("-", 1)[0]

    # Parse the cleaned timestamp
    datetime_obj = datetime.strptime(cleaned_timestamp, "%Y-%m-%dT%H:%M:%S")

    # Convert to Unix timestamp
    return int(datetime_obj.timestamp())

async def send_log(text):
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=text)
        webhook = discord.Webhook.from_url(discord_webhook_log_url, session=session)
        await webhook.send(embed=embed)

if __name__ == "__main__":
    asyncio.run(send_log("Script started"))
    while True:
        asyncio.run(start())
        time.sleep(check_feed_delay)