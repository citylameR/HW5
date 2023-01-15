import psycopg2
from pprint import pprint


def create_db(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(20),
        lastname VARCHAR(30),
        email VARCHAR(254)
        );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        phone_number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)
    return


def add_phone(cur, client_id, phone_number):
    cur.execute("""
        INSERT INTO phonenumbers(phone_number, client_id)
        VALUES (%s, %s);
        """, (phone_number, client_id))
    return client_id


def add_client(cur, name=None, surname=None, email=None, phone_number=None):
    cur.execute("""
        INSERT INTO clients(name, lastname, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    cur.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)
    id = cur.fetchone()[0]
    if phone_number is None:
        return id
    else:
        add_phone(cur, id, phone_number)
        return id


def change_client(cur, id, name=None, lastname=None, email=None):
    cur.execute("""
        SELECT * FROM clients
        WHERE id = %s
        """, (id,))
    info = cur.fetchone()
    if name is None:
        name = info[1]
    if lastname is None:
        lastname = info[2]
    if email is None:
        email = info[3]
    cur.execute("""
        UPDATE clients
        SET name = %s, lastname = %s, email = %s
        WHERE id = %s
        """, (name, lastname, email, id))
    return id


def delete_phone(cur, phone_number):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE phone_number = %s
        """, (phone_number,))
    return phone_number


def delete_client(cur, id):
    cur.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id,))

    cur.execute("""
        DELETE FROM clients
        WHERE id = %s
    """, (id,))
    return id


def find_client(cur, name=None, lastname=None, email=None, phone_number=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if lastname is None:
        lastname = '%'
    else:
        lastname = '%' + lastname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if phone_number is None:
        cur.execute("""
            SELECT id, name, lastname, email, phone_number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s
            """, (name, lastname, email))
    else:
        cur.execute("""
            SElECT id, name, lastname, email, phone_number FROM clients c 
            LEFT JOIN phonenumbers p on c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s AND p.phone_number LIKE %s
        """, (name, lastname, email, phone_number))
    return cur.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(dbname="****", user="postgres", password="*****") as conn:
        with conn.cursor() as cur:
            # 1. Создаем БД
            create_db(cur)
            print('База данных создана')

            # 2. Добавляем клиентов
            print("Добавлен клиент id: ",
                  add_client(cur, 'Andrew', 'Chehov', 'ache@gmail.com', 89991232254))
            print("Добавлен клиент id: ",
                  add_client(cur, 'Alexa', 'Amazonovna', 'Amale@gmail.com', 89181232266))
            print("Добавлен клиент id: ",
                  add_client(cur, 'David', 'Juden', 'goliath_killer@greci.com', 89252189522))
            print("Добавлен клиент id: ",
                  add_client(cur, 'Adofl', '******', 'DreiReich@gmail.com', 89991232255))
            print("Добавлен клиент id: ",
                  add_client(cur, 'Lenin', 'Ulianov', 'CPSU@USSR.com', 38011591519))
            print("Данные в таблицах")
            cur.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.phone_number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(cur.fetchall())
            # 3. Добавляем номер телефона
            print("Телефон добавлен клиенту с id:",
                  add_phone(cur, 33, 3812133133))
            print("Телефон добавлен клиенту с id:",
                  add_phone(cur, 32, 4471234567))
            print("Данные в таблицах")
            cur.execute("""
               SELECT c.id, c.name, c.lastname, c.email, p.phone_number FROM clients c
               LEFT JOIN phonenumbers p ON c.id = p.client_id
               ORDER by c.id
               """)
            pprint(cur.fetchall())
            # 4. Изменить данные клиента
            print("Изменены данные клиента id: ",
                  change_client(cur, 4, "Иван", "None", '123@outlook.com'))
            # 5. Удаляем клиенту номер телефона
            print("Телефон удалён c номером: ",
                  delete_phone(cur, '3812133133'))
            print("Данные в таблицах")
            cur.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.phone_number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(cur.fetchall())
            # 6. Удалим клиента номер 33
            print("Клиент удалён с id: ",
                  delete_client(cur, 33))
            cur.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.phone_number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(cur.fetchall())
            # 7. Найдём клиента
            print('Найденный клиент по имени:')
            pprint(find_client(cur, 'Alexa', ))

            print('Найденный клиент по email:')
            pprint(find_client(cur, None, None, 'goliath_killer@greci.com'))
