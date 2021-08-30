import os
import discord
import json
from enum import Enum
import pathlib
import asyncio
import random
from datetime import date, datetime
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
import psycopg2

# Environment variables
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
PREFIX = os.getenv('DISCORD_PREFIX')

DATABASE_URL = os.environ['DATABASE_URL']

intents = discord.Intents.default()
intents.members = True
intents.presences = True

client = discord.Client(intents=intents)

class Status(str, Enum):
    SLEEP = 'sleep'
    IN = 'in'
    OUT = 'out'

class Command(str, Enum):
    SLEEP = PREFIX+Status.SLEEP.value
    STATUS = PREFIX+'status'
    IN = PREFIX+Status.IN.value
    OUT = PREFIX+Status.OUT.value
    KILL = PREFIX+'kill'

class Availability:
    def __init__(self, status, available_at = None):
        self.status = status
        if status==Status.IN:
            self.available_at = available_at

fileAccessLock = asyncio.Lock()
jsonSuffix = '.json'
membersFileName = GUILD + jsonSuffix

################################################################
def nowStr():
    return f'[{datetime.now().strftime("%d/%m/%Y %H:%M:%S")}]'

def getTime(members, id):
    return members[id][2]

def setTime(members, id, time):
    members[id][2] = time

def getStatus(members, id):
    return members[id][1]

def setStatus(members, id, status):
    members[id][1] = status

def getName(members, id):
    return members[id][0]

def setName(members, id, name):
    members[id][0] = name

#################################################################
async def createGuildTable():
    # Connect to an existing database
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    
    # Open a cursor to perform database operations
    cursor = conn.cursor()
    
    # Execute a command: this sets the correct datestyle
    cursor.execute("ALTER DATABASE das8na0rkcn7fq SET datestyle TO 'ISO, european';")
    
    # Execute a command: this creates a new table
    cursor.execute("CREATE TABLE IF NOT EXISTS oustad (id serial PRIMARY KEY, guild varchar UNIQUE, status varchar NOT NULL, timestamp timestamp);")
    
    # Make the changes to the database persistent
    conn.commit()
    
    # Close communication with the database
    cursor.close()
    conn.close()
    
async def writeMembers(members):
    membersJson = json.dumps(members, indent=4, sort_keys=True)
    async with fileAccessLock:
        
        # Connect to an existing database
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        
        # Open a cursor to perform database operations
        cursor = conn.cursor()
        
        # Pass data to fill a query placeholders and let Psycopg perform
        # the correct conversion (no more SQL injections!)
        cursor.execute("INSERT INTO oustad (guild, status, timestamp) VALUES (%s, %s, %s) ON CONFLICT (guild) DO UPDATE SET status=EXCLUDED.status, timestamp=EXCLUDED.timestamp;", (GUILD, membersJson, datetime.now()))
        
        # Make the changes to the database persistent
        conn.commit()
        
        # Close communication with the database
        cursor.close()
        conn.close()

async def getLastModificationDatetime():
    async with fileAccessLock:
        # Connect to an existing database
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        
        # Open a cursor to perform database operations
        cursor = conn.cursor()
        
        # Query the database and obtain data as Python objects
        cursor.execute("SELECT timestamp FROM oustad where guild=%s", (GUILD,))
        timestamp = cursor.fetchone()
        
        # Close communication with the database
        cursor.close()
        conn.close()
        
        return timestamp
        
async def readMembers():
    async with fileAccessLock:
        # Connect to an existing database
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
        
        # Open a cursor to perform database operations
        cursor = conn.cursor()
        
        # Query the database and obtain data as Python objects
        cursor.execute("SELECT status FROM oustad WHERE guild=%s;", GUILD)
        status = cursor.fetchone()[0]
        
        # Close communication with the database
        cursor.close()
        conn.close()
        
        return json.load(status)

async def requestMembers():
    print(f'{nowStr()} Members dictionary retrieved from the web')
    guild = discord.utils.get(client.guilds, name=GUILD)
    members = {str(key.id): list([key.name, Status.SLEEP, None]) for key in guild.members}
    del members[str(client.user.id)]
    await writeMembers(members)
    return members

async def retrieveMembers():
    lastModificationDatetime = await getLastModificationDatetime()
    if lastModificationDatetime == None or lastModificationDatetime[0].date() != datetime.today().date():
        return await requestMembers()
    else:
        return await readMembers()

##################################################################
def getMembersPerStatus(members, status):
    return dict(filter(lambda member : member[1][1] == status, members.items()))

def getCountPerStatus(members, status):
    return len(dict(filter(lambda member : member[1][1] == status, members.items())))

def membersToString(members, status):
    return ','.join(f'{value[0]}' for value in getMembersPerStatus(members, status.value).values())

def playersNamesPerTime(members):
    players = getMembersPerStatus(members, Status.IN)
    per_time = dict()
    for player in players.items():
        if (player[1][2] is not None and player[1][2] in per_time):
            per_time[player[1][2]].append(player[1][0])
        elif (player[1][2] is not None and player[1][2] not in per_time):
            per_time[player[1][2]] = [player[1][0]]
        elif (player[1][2] is None and 'unspecified' in per_time):
            per_time['unspecified'].append(player[1][0])
        elif (player[1][2] is None and 'unspecified' not in per_time):
            per_time['unspecified'] = [player[1][0]]
    return per_time

async def giveStatus():
    members = await retrieveMembers()
    players = json.dumps(playersNamesPerTime(members), sort_keys=True)
    playersCount = getCountPerStatus(members, Status.IN)
    loosers = membersToString(members, Status.OUT)
    loosersCount = getCountPerStatus(members, Status.OUT)
    sleepers = membersToString(members, Status.SLEEP)
    sleepersCount = getCountPerStatus(members, Status.SLEEP)
    return (
        f'{playersCount} players : {players}\n'
        f'{loosersCount} loosers : {loosers}\n'
        f'{sleepersCount} sleepers : {sleepers}'
        )

###################################################################
def buildProgressMessage(voter, players_counter):
    if (players_counter < 9):
        return (f'Thanks {voter} for voting')
    else:
        return (f'We are {players_counter} available players. Get ready and don\'t be late {voter}!!')

async def sendProgressMessage(message, players_counter):
    response = buildProgressMessage(message.author, players_counter)
    await message.channel.send(response)

async def extractTime(message):
    argument_list = message.content.split()
    if (len(argument_list)>1):
        time_str = argument_list[1]
        try:
            consistent_time = parse(time_str).strftime("%H:%M")
            return consistent_time
        except ParserError:
            print('parsing error')
            await message.channel.send('7mar safi! Wrong time format try again (ex.21:30)')
        

async def updateNewPlayer(members, message, new_status):
    id = str(message.author.id)
    name = message.author.name
    new_time = await extractTime(message)
    if (getTime(members, id) == new_time and getStatus(members, id) == new_status):
        return
    if (getStatus(members, id) != new_status):
        setStatus(members, id, new_status)
    if (getTime(members, id) != new_time):
        setTime(members, id, new_time)
    setName(members, id, name)
    print(f'{nowStr()} {name} is {getStatus(members, id)} at {getTime(members, id)}')
    await writeMembers(members)
    players_counter = sum(value[1] == Status.IN for value in members.values())
    await sendProgressMessage(message, players_counter)
    
#################################################################
@client.event
async def on_ready():
    try:
        await createGuildTable()
        await retrieveMembers()
        guild = discord.utils.get(client.guilds, name=GUILD)
        target_channel = discord.utils.get(guild.channels, name='general')
        print(f'{nowStr()} {client.user} is connected to {guild.name}#{target_channel.name} (id: {guild.id}#{target_channel.id})')
        rules = (
            f'I am counting daily votes to ease the organization of our Among us gaming sessions. Don\'t forget to vote during the day if you miss playing with geniuses like Zine.\n'
            f'New feature! Type {Command.IN.value} 21:30 if you are available only starting 21:30 for example\n'
            f'Type {Command.IN.value} in the chat if you are unemployed\n'
            f'Type {Command.OUT.value} if you are not available tonight'
            )
        await target_channel.send(rules)
    except RuntimeError:
        print(f'Runtime exception : {sys.exc_info()[0]}')

def isCommand(message):
    return message.content.startswith(PREFIX)

def extractCommand(message):
    command = list(filter(lambda single_command : message.content.lower().startswith(single_command.value), list(Command)))
    if len(command):
        return command[0]
    else:
        return Command.SLEEP

async def test(message, members):
    print(playersPerTime(members))

@client.event
async def on_message(message):
    try:
        if message.author == client.user:
            return

        if not isCommand(message):
            return

        members = await retrieveMembers()
        command = extractCommand(message)
        if command==Command.STATUS:
            print(f'{nowStr()} {message.author.name} asked for status')
            await message.channel.send(await giveStatus())
        elif (command==Command.OUT):
            await updateNewPlayer(members, message, Status.OUT)
        elif (command==Command.IN):
            await updateNewPlayer(members, message, Status.IN)
        elif (command==Command.KILL and message.author.name == 'ErgoReda'):
            print(f'Killing bot')
            await client.logout()
        elif (command==Command.SLEEP):
            if message.content.startswith('!test'):
                await test(message, members)
            if message.author.name == 'Isma1l':
                drunk_possible_answers = [
                f'{message.author.name}! Bars are closed. Sorry',
                f'Bars are closed. Sorry',
                f'{message.author.name}! Bars are closed. Sorry',
                f'Bars are closed. Sorry',
                f'{message.author.name}! Bars are closed. Sorry',
                f'Bars are closed. Sorry',
                f'You still here {message.author.name}? I think bars reopened',
                f'Ok hhhhhhhhh. Look a beer!'
                ]
                await message.channel.send(random.choice(drunk_possible_answers))
            
    except RuntimeError:
        print(f'Runtime exception : {sys.exc_info()[0]}')

##################################################################
async def addMember(member):
    members = await retrieveMembers()
    members[str(member.id)] = list([member.name, Status.SLEEP, None])
    print(f'{nowStr()} New member {member.name} added')
    await writeMembers(members)

@client.event
async def on_member_join(member):
    try:
        print(f'Adding : {member}')
        await addMember(member)
        guild = discord.utils.get(client.guilds, name=GUILD)
        target_channel = discord.utils.get(guild.channels, name='general')
        rules = (
            f'I am counting daily votes to ease the organization of our Among us gaming sessions. Don\'t forget to vote during the day if you miss playing with geniuses like Zine.\n'
            f'New feature! Type {Command.IN.value} 21:30 if you are available only starting 21:30 for example\n'
            f'Type {Command.IN.value} in the chat if you are unemployed\n'
            f'Type {Command.OUT.value} if you are not available tonight'
            )
        await target_channel.send(
            f'Hi {member.name}, welcome to the {GUILD} server!\n'
            f'{rules}'
        )
    except RuntimeError:
        print(f'Runtime exception : {sys.exc_info()[0]}')

##################################################################
async def removeMember(member):
    print(f'{nowStr()} Removing {member.name}')
    members = await retrieveMembers()
    members.pop(str(member.id), None)
    await writeMembers(members)

@client.event
async def on_member_remove(member):
    try:
        print(f'Removing :\n {member}')
        await removeMember(member)
        guild = discord.utils.get(client.guilds, name=GUILD)
        target_channel = discord.utils.get(guild.channels, name='general')
        await target_channel.send(
            f'Bye bye tay tay {member.name}!'
        )
    except RuntimeError:
        print(f'Runtime exception : {sys.exc_info()[0]}')

###############################################################
client.run(TOKEN)
