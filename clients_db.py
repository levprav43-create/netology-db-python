import psycopg2
from psycopg2.sql import SQL, Identifier


def create_db(conn):
    """Создаёт таблицы clients и telephones"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                name VARCHAR(40) NOT NULL,
                surname VARCHAR(40) NOT NULL,
                email VARCHAR(320) UNIQUE NOT NULL,
                CONSTRAINT proper_email CHECK (email ~* '^[A-Za-z0-9._+%-]+@[A-Za-z0-9.-]+[.][A-Za-z]+$')
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telephones (
                id SERIAL PRIMARY KEY,
                number VARCHAR(20) UNIQUE NOT NULL,
                client INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE
            );
        """)
    conn.commit()


def add_client(conn, name, surname, email, phones=None):
    """Добавляет клиента. Проверяет уникальность email."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM clients WHERE email = %s;", (email,))
        if cur.fetchone():
            print(f"Клиент с email {email} уже существует")
            return None
        cur.execute("""
            INSERT INTO clients (name, surname, email)
            VALUES (%s, %s, %s) RETURNING id;
        """, (name, surname, email))
        client_id = cur.fetchone()[0]
        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
        conn.commit()
        print(f"ID созданного клиента: {client_id}")
        return client_id


def add_phone(conn, client_id, phone):
    """Добавляет телефон. Проверяет существование клиента и уникальность номера."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM telephones WHERE number = %s;", (phone,))
        if cur.fetchone():
            print(f"Телефон {phone} уже существует")
            return
        cur.execute("SELECT 1 FROM clients WHERE id = %s;", (client_id,))
        if not cur.fetchone():
            print(f"Клиент с ID {client_id} не найден")
            return
        cur.execute("""
            INSERT INTO telephones (number, client)
            VALUES (%s, %s);
        """, (phone, client_id))
        conn.commit()
        print(f"Телефон {phone} добавлен клиенту {client_id}")


def change_client(conn, client_id, name=None, surname=None, email=None):
    """Изменяет данные клиента."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM clients WHERE id = %s;", (client_id,))
        if not cur.fetchone():
            print(f"Клиент с ID {client_id} не найден")
            return
    updates = {'name': name, 'surname': surname, 'email': email}
    for field, value in updates.items():
        if value is not None:
            with conn.cursor() as cur:
                cur.execute(SQL("UPDATE clients SET {} = %s WHERE id = %s").format(Identifier(field)),
                            (value, client_id))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, surname, email FROM clients WHERE id = %s;", (client_id,))
        print(f"Обновлённая запись клиента: {cur.fetchone()}")


def delete_phone(conn, client_id, phone):
    """Удаляет телефон клиента."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM telephones WHERE client = %s AND number = %s;", (client_id, phone))
        if cur.rowcount == 0:
            print("Телефон не найден")
        else:
            conn.commit()
            print(f"Телефон {phone} удалён у клиента {client_id}")


def delete_client(conn, client_id):
    """Удаляет клиента и все его телефоны."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM clients WHERE id = %s;", (client_id,))
        if cur.rowcount == 0:
            print("Клиент не найден")
        else:
            conn.commit()
            print(f"Клиент {client_id} удалён")


def find_client(conn, name=None, surname=None, email=None, phone=None):
    """Ищет клиента по любому параметру."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.name, c.surname, c.email, t.number
            FROM clients c
            LEFT JOIN telephones t ON c.id = t.client
            WHERE (%(name)s IS NULL OR c.name = %(name)s)
              AND (%(surname)s IS NULL OR c.surname = %(surname)s)
              AND (%(email)s IS NULL OR c.email = %(email)s)
              AND (%(phone)s IS NULL OR t.number = %(phone)s)
            ORDER BY c.id;
        """, {'name': name, 'surname': surname, 'email': email, 'phone': phone})
        results = cur.fetchall()
        if results:
            print("Найденные записи:")
            for row in results:
                print(row)
        else:
            print("Клиент не найден")


if __name__ == "__main__":
    # Подключение (измени пароль, если нужно!)
    with psycopg2.connect(
        database="clients_db",
        user="postgres",
        password="postgres"
    ) as conn:
        create_db(conn)
        add_client(conn, "Иван", "Иванов", "ivan@example.com", ["+79001112233"])
        add_client(conn, "Петр", "Петров", "petr@example.com", ["+79004445566"])
        add_phone(conn, 1, "+79000001122")
        change_client(conn, 1, name="Степан")
        delete_phone(conn, 1, "+79001112233")
        find_client(conn, name="Степан")
