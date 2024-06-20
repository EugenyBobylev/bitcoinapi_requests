from pgnotify import await_pg_notifications

from config import Config

if __name__ == '__main__':
    user = Config().dbuser
    pg_uri = Config().get_asyncpg_url()
    print(pg_uri)
    for notification in await_pg_notifications(pg_uri, ['df_msg_inserts']):
        print(notification.channel)
        print(notification.payload)
