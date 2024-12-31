import asyncio
from datetime import datetime, timedelta
import re
import discord
import aiohttp
import requests
import os
import boto3
from dateutil import parser

check_feed_delay = 60
feed_url = "https://api.open511.gov.bc.ca/events?area_id=drivebc.ca/2"
discord_webhook_url = os.environ['DISCORD_WEBHOOK_URL']
discord_webhook_log_url = os.environ['DISCORD_WEBHOOK_LOG_URL']

def lambda_handler(event, context):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start())
    finally:
        loop.close()
    return {
        'statusCode': 200,
        'body': 'Lambda function executed successfully'
    }

async def start():
    # Create database clients
    dynamodb = boto3.resource('dynamodb')
    database_name_last_updated = os.environ['DB_NAME_LAST_UPDATED']
    table_last_updated = dynamodb.Table(database_name_last_updated)
    database_name_active = os.environ['DB_NAME_ACTIVE']
    table_active = dynamodb.Table(database_name_active)

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


    incidents = table_active.scan()['Items']
    incidents2 = []
    for incident in incidents:
        incidents2.append(incident.get('event-id'))


    events_last_updated: dict = table_last_updated.scan()['Items']
    events_last_updated2 = []
    for event in events_last_updated:
        events_last_updated2.append(event.get('event-id'))

    for event in parsed_api['events']:

        event_id = event['id']
        event_updated = event['updated']

        event_last_updated_item = next((item for item in events_last_updated if item['event-id'] == event_id), None)
        if event_last_updated_item is not None:
            event_last_updated = event_last_updated_item['last-updated']
            if event_last_updated != event_updated:
                table_last_updated.put_item(Item={'event-id': event_id, 'last-updated': event_updated})
                await check_if_should_be_notified(event=event, title_prefix="Updated")
        else:
            table_last_updated.put_item(Item={'event-id': event_id, 'last-updated': event_updated})
            await check_if_should_be_notified(event=event, title_prefix="New")
            if event['headline'] == 'INCIDENT':
                incidents.append(event_id)
                table_active.put_item(Item={'event-id': event_id})

        if event['headline'] == 'INCIDENT':
            try:
                incidents2.remove(event_id)
            except ValueError:
                pass

        try:
            events_last_updated2.remove(event_id)
        except ValueError:
            pass


    for incident in incidents2:
        await send_webhook_removed(incident)
        table_active.delete_item(Key={'event-id': incident})

    for event in events_last_updated2:
        print(f"removing {event}")
        table_last_updated.delete_item(Key={'event-id': event})

    await send_log("Script Completed")

async def check_if_should_be_notified(event, title_prefix):
    if event['headline'] == 'INCIDENT':
        await send_webhook(trigger="Incident", event=event, title_prefix=title_prefix)
    elif "Closure" in event['description'] or 'closure' in event['description'] or 'closed' in event['description'] or 'impassible' in event['description']:
        await send_webhook(trigger="Closure Involved", event=event, title_prefix=title_prefix)

async def send_webhook(trigger, event, title_prefix):
    event_short_id = event['id'].split('/')[1]

    # Get "Last Updated" and "Next Update" timestamps in unix style from event
    unix_timestamps = await get_unix_timestamps_from_event(event)

    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=f"{title_prefix} DriveBC Event")
        embed.add_field(name="ID", value=event_short_id.upper())
        embed.add_field(name="Triggered By", value=trigger)
        embed.add_field(name="Direction", value=event['roads'][0]['direction'])
        embed.add_field(name="Road", value=event['roads'][0]['name'])
        embed.add_field(name="From", value=event['roads'][0]['from'])
        embed.add_field(name="To", value=event['roads'][0]['to'])
        embed.add_field(name="Description", value=event['description'], inline=False)
        embed.add_field(name="Last Updated", value=f"<t:{unix_timestamps[1]}:R>")

        if unix_timestamps[0] is None:
            embed.add_field(name="Next Update", value=f"N/A")
        else:
            embed.add_field(name="Next Update", value=f"<t:{unix_timestamps[0]}:R>")

        embed.add_field(name="Links", value=f"https://beta.drivebc.ca/?type=event&id={event_short_id}")
        embed.set_footer(text="https://www2.gov.bc.ca/gov/content/data/policy-standards/open-data/open-government-licence-bc")
        webhook = discord.Webhook.from_url(discord_webhook_url, session=session)
        await webhook.send(embed=embed)

async def send_webhook_removed(event_id):
    event_short_id = event_id.split('/')[1]
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=f"Removed DriveBC Event")
        embed.add_field(name="ID", value=event_short_id.upper())
        embed.set_footer(text="https://www2.gov.bc.ca/gov/content/data/policy-standards/open-data/open-government-licence-bc")
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
        shifted_datetime = datetime_obj + timedelta(hours=8)  # Add 8-hour shift
        return int(shifted_datetime.timestamp())  # Return shifted unix value of the datetime object

    elif len(event_description_split[-5]) == 1 or len(event_description_split[-5]) == 2 and event_description_split[-5].isnumeric():  # If description does not contain a year
        pattern = r"(\w{3}) (\w{3}) (\d{1,2}) at (\d{1,2}:\d{2} [APM]{2}) PST$"
        match = re.search(pattern, description)
        if not match:
            return None
        day_of_week, month, day, time_part = match.groups()
        current_year = datetime.now().year
        timestamp_str = f"{day_of_week} {month} {day}, {current_year} at {time_part}"
        datetime_obj = datetime.strptime(timestamp_str, "%a %b %d, %Y at %I:%M %p")
        shifted_datetime = datetime_obj + timedelta(hours=8)  # Add 8-hour shift
        return int(shifted_datetime.timestamp())  # Return shifted unix value of the datetime object

    else:
        return None

# Get unix timestamp from a timestamp like: 2024-12-19T13:40:20-08:00
async def get_unix_timestamp_from_timestamp(timestamp):
    # Parse the timestamp with timezone information
    datetime_obj = parser.isoparse(timestamp)

    # Convert to Unix timestamp
    return int(datetime_obj.timestamp())

async def send_log(text):
    async with aiohttp.ClientSession() as session:
        embed = discord.Embed(title=text)
        webhook = discord.Webhook.from_url(discord_webhook_log_url, session=session)
        await webhook.send(embed=embed)

if __name__ == "__main__":
    # asyncio.run(send_log("Script started"))
    asyncio.run(start())

    # while True:
    #     asyncio.run(start())
    #     time.sleep(check_feed_delay)