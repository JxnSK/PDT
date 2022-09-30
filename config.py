class Connection:
    hostname = "localhost"
    port = "5432"
    username = "postgres"
    password = "13254Janos"
    database_name = "tweet_conversations"

db_str = f'postgresql://{Connection.username}:{Connection.password}@{Connection.hostname}:{Connection.port}/{Connection.database_name}'
