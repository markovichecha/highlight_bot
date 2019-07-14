from aiohttp import web, ClientSession

import logging
import configparser
import asyncio
import json
import sqlite3
import time


class Config:

    def __init__(self):
        self.settings = configparser.ConfigParser()
        self.settings.read('main.ini')

    def get(self, option, section):
        return self.settings.get(section, option)


class Database:

    def __init__(self, db_name):
        self.database = sqlite3.connect(db_name)
        self.create_table()

    async def store_message(self, values):
        cursor = self.database.cursor()
        query = "INSERT INTO messages (id, chat_id, timestamp) VALUES(?, ?, ?)"
        cursor.execute(query, (values['id'], values['chat_id'], values['timestamp']))
        self.database.commit()

    async def increment_message_rating(self, id):
        cursor = self.database.cursor()
        query = "UPDATE messages SET rating = rating + 1 WHERE id = {}".format(id)
        cursor.execute(query)
        self.database.commit()

    async def get_message_by_id(self, id):
        cursor = self.database.cursor()
        query = "SELECT * FROM messages WHERE id = {}".format(id)
        cursor.execute(query)
        return cursor.fetchone()

    async def get_rated_messages_by_chat(self, chat_id, limit=5):
        cursor = self.database.cursor()
        query = '''SELECT id FROM messages
                        WHERE chat_id = {} and rating > 0
                        ORDER BY rating DESC
                        LIMIT {}'''.format(chat_id, limit)
        cursor.execute(query)
        return cursor.fetchall()

    async def get_rated_messages_by_chat_and_time(self, chat_id, timestamp, limit=5):
        cursor = self.database.cursor()
        query = '''SELECT id FROM messages
            WHERE chat_id = {} and timestamp >= {} and rating > 0 
            ORDER BY rating DESC
            LIMIT {}'''.format(chat_id, timestamp, limit)
        cursor.execute(query)
        return cursor.fetchall()

    def get_last_message_id(self):
        cursor = self.database.cursor()
        query = "SELECT MAX(id) FROM messages"
        cursor.execute(query)
        return cursor.fetchone()

    def create_table(self):
        cursor = self.database.cursor()
        query = '''CREATE TABLE IF NOT EXISTS "messages" (
            "id"	INTEGER NOT NULL,
            "chat_id"	INTEGER NOT NULL,
            "rating"	INTEGER NOT NULL DEFAULT 0,
            "timestamp"	INTEGER NOT NULL,
            PRIMARY KEY("id")
        )'''
        cursor.execute(query)
        self.database.commit()


class Server:

    def __init__(self):
        self.config = Config()
        self.database = Database(self.config.get('name', 'DATABASE'))
        self.last_id = self.database.get_last_message_id()[0] or 0
        self.hostname = self.config.get('hostname', 'TELEGRAM')
        self.bot_token = self.config.get('token', 'TELEGRAM')
        self.proxy = self.config.get('proxy', 'TELEGRAM')
        self.query_url = 'https://api.telegram.org/bot{}/'.format(self.bot_token)

    async def set_webhook(self):
        async with ClientSession() as session:
            webhook_url = 'https://{}/{}'.format(self.hostname, self.bot_token)
            get_url = self.query_url + 'getWebhookInfo'
            async with session.get(get_url, proxy=self.proxy) as resp:
                response = await resp.text()
                data = json.loads(response)
                if not data.get('ok'):
                    logging.warning('An error occurred during checking webhook subscription.')
                    raise Exception
                if data['result'].get('url') == webhook_url:
                    return
            set_url = self.query_url + 'setWebhook'.format(webhook_url)
            async with session.post(set_url, data={'url': set_url, 'allowed_updates': ['message']}, proxy=self.proxy) as resp:
                response = await resp.text()
                data = json.loads(response)
                if not data.get('ok'):
                    logging.warning('An error occurred during the webhook subscription.')
                    raise Exception

    async def send_command(self, chat_id, message_ids):
        message_url = self.query_url + 'sendMessage'
        async with ClientSession() as session:
            order_id = 1
            for message_id in message_ids:
                data = {
                    'chat_id': chat_id,
                    'text': '#{}'.format(order_id),
                    'reply_to_message_id': message_id[0]
                }
                async with session.post(message_url, data=data, proxy=self.proxy) as resp:
                    response = await resp.text()
                    data = json.loads(response)
                    if not data.get('ok'):
                        logging.warning('An error occurred during sending message.')
                order_id += 1

    async def process_command(self, command, chat_id):
        message_ids = None

        if command == 'today':
            timestamp = time.time() - 86400
            message_ids = await self.database.get_rated_messages_by_chat_and_time(chat_id, timestamp)
        if command == 'hour':
            timestamp = time.time() - 3600
            message_ids = await self.database.get_rated_messages_by_chat_and_time(chat_id, timestamp)
        if command == 'best':
            message_ids = await self.database.get_rated_messages_by_chat(chat_id)

        if message_ids:
            await self.send_command(chat_id, message_ids)

    async def process_message(self, message):
        data = {}
        update_id = 0

        data['id'] = message['message_id']
        data['chat_id'] = message['chat']['id']
        data['timestamp'] = message['date']

        text = message.get('text')
        command = text[1:] if text and text[0] == '/' else None
        if command:
            await self.process_command(command, data['chat_id'])

        reply_message = message.get('reply_to_message')
        if reply_message:
            update_id = reply_message['message_id']

        return data, update_id

    async def handle(self, request):
        response = await request.text()
        data = json.loads(response)

        message = data.get('message')
        if message:
            values, update_id = await self.process_message(message)
            if values['id'] > self.last_id:
                await self.database.store_message(values)
                self.last_id = values['id']
                if update_id and await self.database.get_message_by_id(update_id):
                    await self.database.increment_message_rating(update_id)

        return web.Response(text='OK')


server = Server()

app = web.Application()
app.add_routes([
    web.post('/{bot_token}', server.handle)
])

event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(server.set_webhook())
web.run_app(app)
