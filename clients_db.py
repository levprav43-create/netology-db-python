import psycopg2
from psycopg2 import sql


def create_db(conn):
    """Создаёт таблицы client и phone"""
    with conn.cursor() as cur:
        # Удаляем таблицы, если существуют (для чистоты при тестировании)
        cur.execute("""
            DROP TABLE IF EXISTS phone;
            DROP TABLE IF EXISTS client;
        """)
        
        # Таблица клиентов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(150) UNIQUE NOT NULL
            );
        """)
        
        # Таблица телефонов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phone (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,
                number VARCHAR(20) NOT NULL
            );
        """)
    conn.commit()


def add_client(conn, first_name, last_name, email, phones=None):
    """Добавляет нового клиента. phones — список строк (может быть None)."""
    with conn.cursor() as cur:
        # Вставляем клиента
        cur.execute("""
            INSERT INTO client (first_name, last_name, email)
            VALUES (%s, %s, %s) RETURNING id;
        """, (first_name, last_name, email))
        
        client_id = cur.fetchone()[0]
        
        # Добавляем телефоны, если есть
        if phones:
            for phone in phones:
                add_phone(conn, client_id, phone)
                
        conn.commit()
        return client_id


def add_phone(conn, client_id, phone):
    """Добавляет телефон для существующего клиента."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phone (client_id, number)
            VALUES (%s, %s);
        """, (client_id, phone))
        conn.commit()


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    """Изменяет данные клиента. Если передан phones — заменяет все телефоны на новые."""
    with conn.cursor() as cur:
        # Обновляем основные данные, если указаны
        if first_name is not None:
            cur.execute("UPDATE client SET first_name = %s WHERE id = %s;", (first_name, client_id))
        if last_name is not None:
            cur.execute("UPDATE client SET last_name = %s WHERE id = %s;", (last_name, client_id))
        if email is not None:
            cur.execute("UPDATE client SET email = %s WHERE id = %s;", (email, client_id))
        
        # Если передан phones — удаляем старые и добавляем новые
        if phones is not None:
            cur.execute("DELETE FROM phone WHERE client_id = %s;", (client_id,))
            for phone in phones:
                add_phone(conn, client_id, phone)
                
        conn.commit()


def delete_phone(conn, client_id, phone):
    """Удаляет конкретный телефон клиента."""
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phone
            WHERE client_id = %s AND number = %s;
        """, (client_id, phone))
        conn.commit()


def delete_client(conn, client_id):
    """Удаляет клиента (и все его телефоны автоматически из-за ON DELETE CASCADE)."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM client WHERE id = %s;", (client_id,))
        conn.commit()


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    """
    Ищет клиента по любому из параметров.
    Возвращает список кортежей: (id, first_name, last_name, email, [phones])
    """
    with conn.cursor() as cur:
        # Начинаем с базового запроса
        query = """
            SELECT c.id, c.first_name, c.last_name, c.email
            FROM client c
        """
        conditions = []
        params = []

        if first_name:
            conditions.append("c.first_name ILIKE %s")
            params.append(first_name)
        if last_name:
            conditions.append("c.last_name ILIKE %s")
            params.append(last_name)
        if email:
            conditions.append("c.email ILIKE %s")
            params.append(email)
        if phone:
            # Если ищем по телефону — нужен JOIN
            query += " JOIN phone p ON c.id = p.client_id"
            conditions.append("p.number ILIKE %s")
            params.append(phone)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY c.id;"

        cur.execute(query, params)
        clients = cur.fetchall()

        # Для каждого клиента получаем список телефонов
        result = []
        for client in clients:
            client_id = client[0]
            cur.execute("SELECT number FROM phone WHERE client_id = %s;", (client_id,))
            phones = [row[0] for row in cur.fetchall()]
            result.append((*client, phones))

        return result


# === ДЕМОНСТРАЦИЯ РАБОТЫ ===
if __name__ == "__main__":
    # Подключение к БД (убедитесь, что БД 'clients_db' существует!)
    try:
        with psycopg2.connect(
            database="clients_db",
            user="postgres",
            password="makinTosh1122"
        ) as conn:

            print("1. Создаём структуру БД...")
            create_db(conn)

            print("\n2. Добавляем клиентов...")
            client1 = add_client(conn, "Иван", "Иванов", "ivan@example.com", ["+79001112233"])
            client2 = add_client(conn, "Петр", "Петров", "petr@example.com", ["+79004445566", "+79007778899"])
            client3 = add_client(conn, "Мария", "Сидорова", "maria@example.com")  # без телефона

            print(f"   Добавлены клиенты: {client1}, {client2}, {client3}")

            print("\n3. Добавляем ещё один телефон Ивану...")
            add_phone(conn, client1, "+79000001122")

            print("\n4. Меняем email Петра и его телефоны...")
            change_client(conn, client2, email="new_petr@example.com", phones=["+79990001122"])

            print("\n5. Удаляем один телефон Ивана...")
            delete_phone(conn, client1, "+79001112233")

            print("\n6. Поиск клиента по имени 'Иван'...")
            found = find_client(conn, first_name="Иван")
            for f in found:
                print(f"   Найден: {f}")

            print("\n7. Поиск клиента по телефону '+79990001122'...")
            found = find_client(conn, phone="+79990001122")
            for f in found:
                print(f"   Найден: {f}")

            print("\n8. Удаляем клиента Марию...")
            delete_client(conn, client3)

            print("\n9. Проверка: поиск всех оставшихся клиентов...")
            all_clients = find_client(conn)
            for ac in all_clients:
                print(f"   Клиент: {ac}")

    except psycopg2.OperationalError as e:
        print("Ошибка подключения к БД:", e)
        print("Убедитесь, что:")
        print("- PostgreSQL запущен")
        print("- Существует БД с именем 'clients_db'")
        print("- Пользователь 'postgres' имеет пароль 'postgres' (или измените параметры подключения)")
