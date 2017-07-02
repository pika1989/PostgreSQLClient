from PostgreSQLClass import PostgreSQLClient


def print_users(client):
    """Test function
    """
    fields = ["id", "login", "password"]
    users = client.read("users", fields)
    for user in users:
        print user


def insert_user(client):
    fields = ["login", "password"]
    client.insert("users", fields, ("user7", "85634"))


def update_user(client):
    set_data = ["login", "Adam"]
    filter_data = ['id', 2]
    client.update("users", set_data, filter_data)


def delete_user(client):
    client.delete("users", "login", "user7")

client = PostgreSQLClient()

print_users(client)

insert_user(client)
print_users(client)

update_user(client)
print_users(client)

delete_user(client)
print_users(client)

