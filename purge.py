import sqlite3

conn = sqlite3.connect('twitch.db')
c = conn.cursor()

c.execute("drop table if exists chat_log")
print("dropped chat_log")

c.execute("drop table if exists top_spam")
print("dropped top_spam")

c.execute("drop table if exists channels")
print("channels dropped")

conn.close()