import asyncio
import os
import re
import socket
from datetime import datetime, timezone
from typing import Union, List

import mysql.connector
import discord
from discord.ext import commands
from dotenv import load_dotenv
from mcstatus import JavaServer

# Load environment variables
load_dotenv('.env')

# Bot Initialization
bot = commands.Bot(command_prefix='$')
servers = []

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    mydb, cursor = connect()
    
    cursor.execute("SELECT id, ip, announce_joins, announce_joins_id FROM servers")
    rows = cursor.fetchall()

    for row in rows:
        if get_server_by_id(row[0]) is None:
            print(f"Kicked: Bot was removed from {row[0]}. Cleaning up.")
            await do_bot_cleanup(row[0])
            continue

        bot.loop.create_task(status_task(row[0]))
        servers.append(row[0])
        announcement_status = "Not announcing" if not row[2] else f"Announcing in <#{row[3]}>"
        print(f"Now querying {get_server_by_id(row[0]).name}. {announcement_status} for {row[1]}")


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    command = ctx.message.content.split()[0][1:]  # Extract command name
    prefix = ctx.prefix

    if isinstance(error, commands.CommandNotFound):
        await safe_send(ctx, f'Command "{command}" does not exist. Use `{ctx.prefix}help` for available commands.')
    elif isinstance(error, commands.MissingRequiredArgument):
        await log(ctx, f'Command "{command}" is missing required arguments. Use `{ctx.prefix}help {command}` for details.')
    elif isinstance(error, commands.MissingPermissions):
        await safe_send(ctx, f'You lack permissions: {", ".join(error.missing_perms)}.')
    elif isinstance(error, commands.CommandInvokeError) and isinstance(error.original, socket.gaierror):
        await log(ctx, "Could not connect, is the server down?")
    else:
        raise error


class Admin(commands.Cog):
    """Admin"""

    @commands.command(brief="Sets up new Minecraft server querier",
                      description="Sets up new Minecraft server querier. Removes previous querier, if applicable. Use a valid domain name or IP address. The default port (25565) is used unless otherwise specified. Specify a channel ID to announce player joins there.")
    @commands.has_permissions(administrator=True)
    async def setup(self, ctx: discord.ext.commands.context.Context, ip: str, port: int = 25565, annChanID: Union[int, None] = None):
        """Sets up a new Minecraft server querier."""
        val_ip, val_port = await validate_ip_port(ctx, ip, port)
        if not val_ip:
            return

        mc = JavaServer.lookup(f"{val_ip}:{val_port}")

        try:
            query = mc.query()
            names = query.players.names
        except asyncio.exceptions.TimeoutError:
            await log(ctx, "Setup query error, query port enabled?")
        except ConnectionRefusedError:
            await log(ctx, "Setup query failed, server down?")
        else:
            mydb, cursor = connect()

            cursor.execute(f"INSERT INTO servers (id, ip, port) VALUES({str(ctx.guild.id)}, \"{val_ip}\", {str(port)}) ON DUPLICATE KEY UPDATE ip=\"{val_ip}\", port={str(port)}")

            setMCNames(ctx.guild.id, cursor, names)

            cursor.execute(f"INSERT INTO times (id) VALUES({ctx.guild.id}) ON DUPLICATE KEY UPDATE id=id;")

            mydb.commit()
            mydb.close()

            if annChanID is None:
                await setAnn(ctx, False)
            else:
                await setAnn(ctx, True, annChanID)

            if ctx.guild.id not in servers:
                bot.loop.create_task(status_task(ctx.guild.id))
                servers.append(ctx.guild.id)

            await log(ctx, f"Setup new server query with IP: {val_ip}")

    @commands.command(brief="Removes querier and status channels",
                      description="Removes querier and status channels. Deletes your guild's data from my server. Data deletion occurs automatically when I am removed from your guild.")
    @commands.has_permissions(administrator=True)
    async def cleanup(self, ctx: discord.ext.commands.context.Context):
        await do_bot_cleanup(ctx.guild.id, ctx=ctx)

    @commands.command(brief="Turns on player join announcements",
                      description="Turns on player join announcements in specified channel.")
    @commands.has_permissions(administrator=True)
    async def announce(self, ctx: discord.ext.commands.context.Context, chanid: int):
        await setAnn(ctx, True, chanid)

    @commands.command(brief="Turns off player join announcements", description="Turns off player join announcements.")
    @commands.has_permissions(administrator=True)
    async def noannounce(self, ctx: discord.ext.commands.context.Context):
        await setAnn(ctx, False)

    @commands.command(brief="Turns on total player hour display", description="Turns on total player hour display.")
    @commands.has_permissions(administrator=True)
    async def showhours(self, ctx: discord.ext.commands.context.Context):
        await setHours(ctx, True)

    @commands.command(brief="Turns off total player hour display", description="Turns off total player hour display.")
    @commands.has_permissions(administrator=True)
    async def nohours(self, ctx: discord.ext.commands.context.Context):
        await setHours(ctx, False)

    # @commands.command()
    # async def notif(ctx: discord.ext.commands.context.Context, kind: Union[str, None] = None):
    #     if kind is None:
    #         await safe_send(ctx, "The notification options are: \n1. joins")
    #     else:
    #         name = ""
    #         if kind.lower() ==  "joins":
    #             name = "Status Joins"
    #
    #         if name != "":
    #             await addrole(ctx, name)
    #             await log(ctx, "Gave", name, "role to", ctx.message.author.name)
    #
    #
    # async def addrole(ctx: discord.ext.commands.context.Context, name: str):
    #     member = ctx.message.author
    #     role = get(member.guild.roles, name=name)
    #     if role is None:
    #         role = await member.guild.create_role(name=name)
    #
    #     await member.add_roles(role)


class Other(commands.Cog):
    """Other"""

    def __init__(self, bot: discord.ext.commands.Bot):
        bot.help_command.cog = self

    @commands.command(brief="Shows server status", description="Shows server status.")
    async def status(self, ctx: discord.ext.commands.context.Context):
        mydb, cursor = connect()
        ip, port = getMCIP(ctx.guild.id, cursor)
        mc = JavaServer(ip, port)
        

        if mc is None:
            await safe_send("There is no server query set up. Run the `setup` command to get started.", ctx=ctx)
        else:
            players, max, _, motd = await getStatus(mc)

            seconds = getPersonSeconds(ctx.guild.id, cursor)
            seconds = seconds % (24 * 3600)
            hour = seconds // 3600
            seconds %= 3600
            minutes = seconds // 60
            seconds %= 60

            await safe_send("Status:\n  " + ip + "\n  " + motd + "\n\n  Players: " + str(players) + "/" + str(max) + "\n  Total Player Time: " + "%d:%02d:%02d" % (hour, minutes, seconds), ctx=ctx, format="```")
            
        mydb.close()

    @commands.command(brief="Lists online players", description="Lists online players.")
    async def players(self, ctx: discord.ext.commands.context.Context):
        mydb, cursor = connect()
        ip, port = getMCIP(ctx.guild.id, cursor)
        mc = JavaServer(ip, port)
        mydb.close()

        if mc is None:
            await safe_send("There is no server query set up. Run the `setup` command to get started.", ctx=ctx)
        else:
            _, _, names, _ = await getStatus(mc)

            pStr = "Online Players:\n"
            for name in names:
                pStr += "  " + name + "\n"

            pStr = pStr[:-1]
            await safe_send(pStr, ctx=ctx, format="```")

    @commands.command(brief="Shows last query time", description="Shows last query time. Useful for debugging server connection issues.")
    async def lastquery(self, ctx: discord.ext.commands.context.Context):
        mydb, cursor = connect()
        ip, port = getMCIP(ctx.guild.id, cursor)
        mc = JavaServer(ip, port)
        last = getMCQueryTime(ctx.guild.id, cursor)
        mydb.close()

        if mc is None:
            await safe_send("There is no server query set up. Run the `setup` command to get started.", ctx=ctx)
        else:
            await safe_send("I last queried " + ip + " at " + str(last) + " UTC", ctx=ctx)


async def setAnn(ctx: discord.ext.commands.context.Context, ann: bool, cid: Union[int, None] = None):
    if cid is not None and find_channels(serv=ctx.guild, chanid=cid) is None:
        await log(ctx, "Channel", str(cid), "does not exist.")
        return

    mydb, cursor = connect()
    ip, _ = getMCIP(ctx.guild.id, cursor)

    cursor.execute(
        "UPDATE servers SET announce_joins=" + str((0, 1)[ann]) + ", announce_joins_id=" + ("NULL", str(cid))[
            ann] + " WHERE id=" + str(ctx.guild.id))

    mydb.commit()
    mydb.close()

    await log(ctx, (
        "Not announcing when a player joins " + ip + ".",
        "Announcing when a player joins " + ip + " in <#" + str(cid) + ">.")[
        ann])


async def setHours(ctx: discord.ext.commands.context.Context, hours: bool):
    mydb, cursor = connect()
    ip, _ = getMCIP(ctx.guild.id, cursor)

    cursor.execute(
        f"UPDATE servers SET hours={str((0, 1)[hours])} WHERE id={str(ctx.guild.id)}")

    mydb.commit()
    mydb.close()

    await log(ctx, (
        "Not displaying total player hours for " + ip + ".",
        "Displaying total player hours for " + ip + ".")[
        hours])


async def log(ctx: commands.Context, *msg: str):
    """Log messages both in Discord and console."""
    print(ctx.guild.name, "Log:", " ".join(msg))
    await safe_send(" ".join(msg), ctx=ctx)


async def validate_ip_port(ctx, ip, port):
    """Validates IP and Port"""
    split = ip.split(":")
    if len(split) > 1:
        ip = split[0]

        try:
            port = int(split[1])
        except ValueError:
            await log(ctx, str(port), "is not a valid port number, please try again.")
            return None, None

    if port not in range(65536):
        await log(ctx, str(port), "is not a valid port number, please try again.")
        return None, None

    domain = re.search("^([a-z0-9]+(-[a-z0-9]+)*\.)+[a-z]{2,6}$", ip)
    addr = re.search(
        "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$",
        ip)
    if domain is None or domain.group(0) != ip:
        if addr is None or addr.group(0) != ip:
            await log(ctx, ip, "is not a valid domain or IP address, please try again.")
            return None, None

    if re.search("(^127\.)|(^10\.)|(^172\.1[6-9]\.)|(^172\.2[0-9]\.)|(^172\.3[0-1]\.)|(^192\.168\.)",
                    ip) is not None:
        await log(ctx, ip, "is a private IP. I won't be able to query it.")
        return None, None
    
    return ip, port

def get_server_by_id(sid: int):
    for server in bot.guilds:
        if server.id == sid:
            return server


def find_channels(serv: Union[discord.Guild, None] = None, sid: Union[int, None] = None,
                  chanid: Union[int, None] = None, channame: Union[str, None] = None, channamesearch: str = "equal",
                  chantype: Union[discord.ChannelType, None] = None):
    matches = []
    checkServer = None

    if sid is not None:
        checkServer = get_server_by_id(sid)

    if serv is not None:
        checkServer = serv

    for c in checkServer.channels:
        if chanid is not None and c.id != chanid:
            continue
        if channame is not None and channamesearch.lower() == "equal" and c.name != channame:
            continue
        if channame is not None and channamesearch.lower() == "in" and channame not in c.name:
            continue
        if chantype is not None and c.type != chantype:
            continue

        matches.append(c)

    return matches


async def getStatus(serv: JavaServer):
    status = await asyncio.wait_for(serv.async_query(), timeout=1.0)
    p = status.players.online
    m = status.players.max
    n = status.players.names
    d = status.motd

    return p, m, n, d


def connect():
    mydb = mysql.connector.connect(
        host="localhost",
        user=os.getenv("USER"),
        password=os.getenv("PASS"),
        database="status"
    )

    cursor = mydb.cursor()
    return mydb, cursor


def getMCIP(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT ip, port FROM servers WHERE id=" + str(sid))
    row = cursor.fetchone()
    ip = row[0]
    port = row[1]

    return ip, port


def getMCNames(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT name FROM names WHERE id=" + str(sid))
    rows = cursor.fetchall()

    return [row[0] for row in rows]


def getMCQueryTime(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT last_query FROM servers WHERE id=" + str(sid))
    tstr = cursor.fetchone()[0]

    return tstr

def getPersonSeconds(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT time FROM times WHERE id=" + str(sid))
    tstr = cursor.fetchone()[0]

    return int(tstr)

def getShowHours(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT hours FROM servers WHERE id=" + str(sid))
    tstr = cursor.fetchone()[0]

    return bool(tstr)

def getMCJoinAnnounce(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection):
    cursor.execute("SELECT announce_joins, announce_joins_id FROM servers WHERE id=" + str(sid))
    row = cursor.fetchone()
    ann = row[0]
    cid = row[1]

    return ann, cid


def setMCNames(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection, names: List[str]):
    cursor.execute("DELETE FROM names WHERE id=" + str(sid))

    if len(names) > 0:
        qStr = ""
        for name in names:
            qStr += "(" + str(sid) + ",\"" + name + "\"),"

        qStr = qStr[:-1] + ";"

        cursor.execute("INSERT INTO names (id, name) VALUES " + qStr)


def setMCQueryTime(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection, dt: datetime):
    cursor.execute("UPDATE servers SET last_query=\"" + dt.strftime('%Y-%m-%d %H:%M:%S') + "\" WHERE id=" + str(sid))

def incrementPersonSeconds(sid: int, cursor: mysql.connector.connection_cext.CMySQLConnection, seconds: int):
    cursor.execute(f"UPDATE times SET time=time+{seconds} WHERE id={str(sid)};")

async def do_bot_cleanup(sid: int, ctx: Union[discord.ext.commands.context.Context, None] = None):
    if sid in servers:
        servers.remove(sid)

    deleted = False
    currServ = get_server_by_id(sid)
    if currServ is not None:
        iChannels = find_channels(serv=currServ, channame="IP: ", channamesearch="in", chantype=discord.ChannelType.voice)
        pChannels = find_channels(serv=currServ, channame="Players: ", channamesearch="in", chantype=discord.ChannelType.voice)
        hChannels = find_channels(serv=currServ, channame="Player Hrs: ", channamesearch="in", chantype=discord.ChannelType.voice)
        channels = iChannels + pChannels + hChannels
        try:
            for channel in channels:
                await channel.delete()

            deleted = True
        except discord.errors.Forbidden:
            print(currServ.name, "Error: I don't have permission to delete channels.")

    mydb, cursor = connect()
    ip, port = getMCIP(sid, cursor)
    mc = JavaServer(ip, port)

    cursor.execute("DELETE FROM servers WHERE id=" + str(sid))
    cursor.execute("DELETE FROM names WHERE id=" + str(sid))
    cursor.execute("DELETE FROM times WHERE id=" + str(sid))

    mydb.commit()
    mydb.close()

    if ctx is None:
        print("Cleaned up", sid, ".")
    else:
        if mc is None:
            await safe_send("There is no server query set up. Run the `setup` command to get started.", ctx=ctx)
        else:
            await log(ctx, "Cleaned up! Removed", ip, "querier, deleted", ctx.guild.name + "'s data from my server,", ("but failed to remove my status channels. ", "and removed my status channels.")[deleted])


async def safe_send(msg: str, ctx: Union[discord.ext.commands.context.Context, None] = None,
                   chan: Union[discord.TextChannel, None] = None, format: str = ''):
    try:
        if ctx is not None:
            await ctx.send(f"{format}{msg}{format}")
        elif chan is not None:
            await chan.send(f"{format}{msg}{format}")
    except discord.errors.Forbidden:
        if ctx is not None:
            print(ctx.guild.name,
                  "Error: I don't have permission to send messages here.")
        elif chan is not None:
            print(chan.guild.name,
                  "Error: I don't have permission to send messages here.")


async def status_task(sid: int):
    first_iter = True

    while True:
        # if bot is not a member, clean data, end task
        if get_server_by_id(sid) is None:
            print("Kicked: Bot must've been kicked from", sid, ". Cleaning up.")
            await do_bot_cleanup(sid)
            return

        # check to see if specified server is still querying
        if sid not in servers:
            print(get_server_by_id(sid).name, "is not querying. Ending task.")
            return

        mydb, cursor = connect()
        ip, port = getMCIP(sid, cursor)
        mc = JavaServer(ip, port)
        currServ = get_server_by_id(sid)
        wait = 10

        if not ip == "" and currServ is not None:
            try:
                oldNames = getMCNames(sid, cursor)
                players, max, names, _ = await getStatus(mc)
                lastTime = getMCQueryTime(sid, cursor)
                lastSeconds = getPersonSeconds(sid, cursor)

                currTime = datetime.now(timezone.utc)
                if lastTime is None:
                    lastTime = currTime
                else:
                    lastTime = lastTime.replace(tzinfo=timezone.utc)
                    
                print(currServ.name, "Query:", ip, str(players) + "/" + str(max), datetime.now(timezone.utc).strftime("%H:%M:%S"),
                      str(currTime - lastTime))

                setMCNames(sid, cursor, names)
                setMCQueryTime(sid, cursor, currTime)
                if not first_iter:
                    incrementPersonSeconds(sid, cursor, len(names) * (currTime - lastTime).total_seconds())

                announceJoin, annChanId = getMCJoinAnnounce(sid, cursor)
                annRole = None

                if announceJoin and annChanId is not None:
                    jNames = list(set(names) - set(oldNames))
                    for name in jNames:
                        aChannels = find_channels(serv=currServ, chanid=int(annChanId))

                        if len(aChannels) > 0:
                            if annRole is None:
                                await safe_send(name + " joined the game!", chan=aChannels[0])
                            else:
                                await safe_send("<@&" + str(annRole.id) + "> " + name + " joined the game!",
                                               chan=aChannels[0])

                            print(currServ.name, "Announced player(s) join.")

            except asyncio.exceptions.TimeoutError:
                print(currServ.name, "Timeout, server lagging?")
                players = -1
                lastSeconds = -1

            except (ConnectionRefusedError, socket.gaierror):
                print(currServ.name, "Cannot connect to server, down?")
                players = -1
                lastSeconds = -1

            iChannels = find_channels(serv=currServ, channame="IP: ", channamesearch="in", chantype=discord.ChannelType.voice)
            pChannels = find_channels(serv=currServ, channame="Players: ", channamesearch="in",
                                      chantype=discord.ChannelType.voice)
            tChannels = find_channels(serv=currServ, channame="Player Hrs: ", channamesearch="in",
                                      chantype=discord.ChannelType.voice)

            overwrites = {
                currServ.default_role: discord.PermissionOverwrite(connect=False, view_channel=True),
                currServ.me: discord.PermissionOverwrite(connect=True, view_channel=True, manage_channels=True)
            }

            if len(iChannels) > 0:
                lastIPName = iChannels[0].name

                ipStr = "IP: " + ip
                if lastIPName != ipStr:
                    try:
                        print(currServ.name, "Update: Ip changed!")
                        await iChannels[0].edit(name=ipStr)
                        wait = 301
                    except discord.errors.Forbidden:
                        print(currServ.name,
                              "Error: I don't have permission to edit channels. Try deleting the channels I create. Then, run the `setup` command again.")
                        await do_bot_cleanup(sid)
                        return
            else:
                await currServ.create_voice_channel("IP: " + ip, overwrites=overwrites)

            if len(pChannels) > 0:
                lastPName = pChannels[0].name

                if players == -1:
                    pStr = lastPName
                else:
                    pStr = "Players: " + str(players) + "/" + str(max)

                if lastPName != pStr:
                    try:
                        print(currServ.name, "Update: Players changed!")
                        await pChannels[0].edit(name=pStr)
                        wait = 301
                    except discord.errors.Forbidden:
                        print(currServ.name,
                              "Error: I don't have permission to edit channels. Try deleting the channels I create. Then, run the `setup` command again.")
                        await do_bot_cleanup(sid)
                        return
            else:
                await currServ.create_voice_channel(f"Players: {str(players)}/{str(max)}",
                                                    overwrites=overwrites)

            if getShowHours(sid, cursor) and not first_iter and lastSeconds != -1:
                tStr = "Player Hrs: " + str(round((lastSeconds + len(names) * (currTime - lastTime).total_seconds())/3600))
                if len(tChannels) > 0:
                    lastTName = tChannels[0].name

                    if lastTName != tStr:
                        try:
                            print(currServ.name, "Update: Time changed!")
                            await tChannels[0].edit(name=tStr)
                            wait = 301
                        except discord.errors.Forbidden:
                            print(currServ.name,
                                "Error: I don't have permission to edit channels. Try deleting the channels I create. Then, run the `setup` command again.")
                            await do_bot_cleanup(sid)
                            return
                else:
                    await currServ.create_voice_channel(tStr,
                                                        overwrites=overwrites)

        mydb.commit()
        mydb.close()

        await asyncio.sleep(wait)
        first_iter = False

bot.add_cog(Admin())
bot.add_cog(Other(bot))
bot.run(os.getenv('TOKEN'))
