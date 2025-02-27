import psycopg2
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


class DbSqlClient:
    def __init__(self, database, user, password):
        self.database = database
        self.user = user
        self.password = password

    def create_table(self, table_name, columns):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name}{columns};
                """, ("Python",))
                conn.commit()
        conn.close()

    def delete_table(self, table_name):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""DROP TABLE {table_name};""", ("Python",))
                conn.commit()
        conn.close()

    def add_info(self, place, info):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                        INSERT INTO {place} 
                        VALUES({info});
                """, ("Python",))
                conn.commit()
        conn.close()

    def find_info(self, place, condition):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                        SELECT * 
                        FROM {place} 
                        WHERE {condition};
                """, ("Python",))
                info = cur.fetchall()
        conn.close()
        return info

    def update_info(self, table, client_id, new_info_type, new_info):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                        UPDATE {table}
                        SET {new_info_type} = '{new_info}'
                        WHERE id = {client_id};
                """, ("Python",))
                conn.commit()
        conn.close()

    def delete_info(self, table, condition):
        with psycopg2.connect(database=self.database, user=self.user, password=self.password) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                        DELETE
                        FROM {table}
                        {condition};
                """, ("Python",))
                conn.commit()
        conn.close()


def create_db_structure(db_client):
    structure = [
        {'table_name': 'client',
         'columns': """(
                            id SERIAL PRIMARY KEY,
                            first_name VARCHAR(40) NOT NULL,
                            last_name VARCHAR(40) NOT NULL,
                            email VARCHAR(40) NOT NULL
                        )"""},
        {'table_name': 'phone',
         'columns': """(
                            id SERIAL PRIMARY KEY,
                            number VARCHAR(15) NULL,
                            client_id INTEGER NOT NULL REFERENCES client(id)
                        )"""}
    ]
    for table in structure:
        db_client.create_table(table['table_name'], table['columns'])


def add_client(db_client, first_name, last_name, email):
    place = "client(first_name, last_name, email)"
    info = f"'{first_name}', '{last_name}', '{email}'"
    db_client.add_info(place, info)


def add_phone(db_client, phone_number, client_id):
    place = "phone(number, client_id)"
    info = f"'{phone_number}', '{client_id}'"
    db_client.add_info(place, info)


def find_client(db_client, condition_type, condition_info):
    if condition_type != 'phone':
        place = 'client'
        condition = f"{condition_type} = '{condition_info}'"
        client = db_client.find_info(place, condition)
    else:
        condition = f"number = '{condition_info}'"
        client_id = db_client.find_info('phone', condition)[0][2]
        condition = f"id = '{client_id}'"
        client = db_client.find_info('client', condition)
    client_id = client[0][0]
    client_phones = db_client.find_info('phone', f"client_id = {client_id}")

    numbers = [phone['number'] for phone in format_phones(client_phones)]
    client_info = format_info(client)[0] | {'phone': numbers}
    return client_info


def format_info(clients):
    formatted_info = []
    for client in clients:
        client_info = {
            'client_id': client[0],
            'first_name': client[1],
            'last_name': client[2],
            'email': client[3]
        }
        formatted_info.append(client_info)
    return formatted_info


def format_phones(phones):
    formatted_info = []
    for phone in phones:
        phone_info = {
            'client_id': phone[2],
            'phone_id': phone[0],
            'number': phone[1]
        }
        formatted_info.append(phone_info)
    return formatted_info


def edit_client(db_client, client_id, new_info_type, new_info):
    table = 'client'
    db_client.update_info(table, client_id, new_info_type, new_info)


def delete_phone(db_client, client_id, number):
    table = 'phone'
    condition = f"WHERE id = {client_id} AND number = '{number}'"
    db_client.delete_info(table, condition)


def delete_client(db_client, client_id):
    table = 'client'
    condition = f'WHERE id = {client_id}'
    db_client.delete_info(table, condition)


if __name__ == '__main__':
    db_sql_client = DbSqlClient(database='hw5_clients', user='postgres', password=config['Passwords']['postgres'])

    # db_sql_client.delete_table('client')
    # db_sql_client.delete_table('phone')
    # create_db_structure(db_sql_client)

    # add_client(db_sql_client, first_name='Name_1', last_name='Surname_1', email='email_1@ya.ru')
    # add_client(db_sql_client, first_name='Name_2', last_name='Surname_2', email='email_2@ya.ru')
    # add_client(db_sql_client, first_name='Name_3', last_name='Surname_3', email='email_3@ya.ru')
    # add_client(db_sql_client, first_name='Name_4', last_name='Surname_4', email='email_4@ya.ru')

    # add_phone(db_sql_client, phone_number='7 495 1111111', client_id=1)
    # add_phone(db_sql_client, phone_number='7 495 1111112', client_id=1)
    # add_phone(db_sql_client, phone_number='7 499 2222222', client_id=2)

    # print('Searching:')
    # print(find_client(db_sql_client, condition_type='id', condition_info=1))
    # print(find_client(db_sql_client, condition_type='id', condition_info='4'))
    # print(find_client(db_sql_client, condition_type='first_name', condition_info='Name_3'))
    # print(find_client(db_sql_client, condition_type='last_name', condition_info='Surname_1'))
    # print(find_client(db_sql_client, condition_type='email', condition_info='email_2@ya.ru'))
    # print('Searching with phone number:')
    # print(find_client(db_sql_client, condition_type='phone', condition_info='7 495 1111111'))
    # print(find_client(db_sql_client, condition_type='phone', condition_info='7 495 1111112'))
    # print(find_client(db_sql_client, condition_type='phone', condition_info='7 499 2222222'))

    # edit_client(db_sql_client, client_id='4', new_info_type='first_name', new_info='Upd name 4')
    # edit_client(db_sql_client, client_id='4', new_info_type='last_name', new_info='Upd surname 4')
    # edit_client(db_sql_client, client_id='1', new_info_type='email', new_info='email_1_upd@ya.ru')

    # delete_phone(db_sql_client, client_id='1', number='7 444 4444444')
    # delete_client(db_sql_client, client_id='1')

    # db_sql_client.delete_table('phone')
