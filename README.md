# DBMS Project

Учебная SQL-СУБД на C++20 с собственным storage engine, HTTP API, консольным клиентом, авторизацией, RBAC, асинхронными запросами и демонстрационным кластерным режимом с шардингом.

Проект реализует полный путь обработки запроса: пользователь вводит SQL в CLI или отправляет JSON по HTTP, сервер парсит SQL через Flex/Bison в `QueryPlan`, проверяет права доступа, выбирает shard/storage node, выполняет операцию в движке хранения и возвращает JSON-ответ.

## Возможности

- SQL-парсер на Flex/Bison.
- HTTP-сервер на Crow.
- Консольный клиент, работающий в интерактивном режиме и в режиме выполнения `.sql`-скрипта.
- Собственный storage engine:
  - базы данных и таблицы на файловой системе;
  - бинарный менеджер файлов;
  - схемы таблиц;
  - B+-деревья для индексированных колонок;
  - оптимизатор запросов с выбором `FULL_TABLE_SCAN`, `INDEX_SEEK` или `INDEX_RANGE_SCAN`;
  - журналирование операций и WAL;
  - восстановление через WAL;
  - `REVERT` таблицы к моменту времени.
- Поддержка базовых SQL-команд: `CREATE DATABASE`, `USE`, `CREATE TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, `DROP`, `REVERT`.
- Поддержка `WHERE`, `AND`, `OR`, `BETWEEN`, `LIKE`, `GROUP BY`, `ORDER BY`, алиасов и агрегатов.
- Асинхронные запросы через `/query_async` и `/async/<guid>`.
- JWT-аутентификация и RBAC-права на уровне базы данных.
- Кластерный режим:
  - in-process shards;
  - внешние storage nodes;
  - consistent hashing;
  - heartbeat-мониторинг;
  - статистика RPS, ошибок и времени ответа.
- Access log для HTTP-запросов и storage-сервера.
- Regression-тесты PowerShell.

## Архитектура

```text
+-------------------+        HTTP JSON        +------------------------+
| dbms_client       |  ---------------------> | dbms_http_server       |
| interactive / SQL |                         | Crow + Auth + Parser   |
+-------------------+                         +-----------+------------+
                                                        |
                                                        | QueryPlan / SQL
                                                        v
                                           +------------+-------------+
                                           | Query Executor           |
                                           | RBAC, async queue, stats |
                                           +------------+-------------+
                                                        |
                         +------------------------------+------------------------------+
                         |                                                             |
                         v                                                             v
          +-----------------------------+                              +-----------------------------+
          | In-process shards           |                              | External storage nodes       |
          | Engine per shard            |                              | TCP binary protocol          |
          +-------------+---------------+                              +--------------+--------------+
                        |                                                              |
                        v                                                              v
          +-----------------------------+                              +-----------------------------+
          | Storage Engine              |                              | storage_server + Engine      |
          | files, schema, WAL, indexes |                              | files, schema, WAL, indexes |
          +-----------------------------+                              +-----------------------------+
```

Основной HTTP-сервер может работать в двух режимах:

1. **Single-node / in-process** — все запросы выполняются внутри процесса `dbms_http_server`.
2. **Sharded / clustered** — запросы маршрутизируются по shard-узлам. Если внешние `storage_server` не заданы, сервер поднимает несколько in-process shard engine по параметру `--cluster-shards`.

## Структура проекта

```text
dbms_project/
├── CMakeLists.txt
├── accounts.json
├── HOW_TO_RUN.txt
├── client/
│   ├── CMakeLists.txt
│   └── main.cpp
├── server/
│   ├── CMakeLists.txt
│   ├── main.cpp
│   ├── network/
│   │   ├── http_server.cpp
│   │   ├── http_server.h
│   │   ├── auth.cpp
│   │   ├── auth.h
│   │   ├── account_store.cpp
│   │   └── account_store.h
│   ├── parser/
│   │   ├── grammar.y
│   │   └── lexer.l
│   ├── storage/
│   │   ├── include/
│   │   ├── src/
│   │   └── tests/
│   └── cluster/
│       ├── include/
│       └── src/
├── shared/
│   └── QueryPlan.h
└── tests/
    ├── sql/coursework_full.sql
    ├── run_regression.ps1
    └── run_advanced_regression.ps1
```

### Основные модули

| Модуль | Назначение |
|---|---|
| `client` | CLI-клиент: читает SQL из консоли или файла и отправляет запросы на HTTP-сервер. |
| `server/network` | HTTP API, маршруты Crow, JWT, аккаунты, RBAC. |
| `server/parser` | Flex/Bison-парсер SQL, который заполняет `QueryPlan`. |
| `server/storage` | Движок хранения: БД, таблицы, записи, индексы, WAL, схемы, агрегаты. |
| `server/cluster` | TCP storage nodes, consistent hashing, heartbeat, async queue, telemetry. |
| `shared` | Общие структуры, прежде всего `QueryPlan`. |
| `tests` | SQL-сценарии и regression-тесты. |

## Требования

- CMake 3.16+ для серверной части, CMake 3.20+ для клиента.
- Компилятор C++20.
- Flex и Bison.
- Threads / pthread.
- Windows: `ws2_32`, `mswsock` подключаются через CMake.
- Сторонние header-only зависимости в `third_party`:
  - Crow;
  - nlohmann/json;
  - cpp-httplib;
  - Boost headers, используемые B+-деревом.

> Примечание: в репозитории ожидается папка `third_party`. Если сборка не находит `crow.h`, `httplib.h`, `nlohmann/json.hpp` или Boost headers, проверьте, что зависимости лежат в путях, указанных в `CMakeLists.txt`.

## Сборка

### Linux / macOS

```bash
cd dbms_project
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j
```

После сборки должны появиться основные цели:

- `dbms_http_server` — основной HTTP-сервер;
- `dbms_client` — консольный клиент;
- `storage_server` — внешний storage node;
- `cluster_entrypoint` — отдельный TCP entrypoint для кластерного модуля;
- `test_binary_file_manager` — тест binary file manager.

### Windows / CLion

1. Откройте папку `dbms_project` в CLion.
2. Дождитесь конфигурации CMake.
3. Выберите конфигурацию `dbms_http_server`.
4. В `Program arguments` укажите, например:

```text
--data-root ./test_data --host 127.0.0.1 --port 18080
```

5. Запустите сервер.
6. Затем выберите конфигурацию `dbms_client` и запустите клиент.

## Быстрый старт

### 1. Запустить сервер без авторизации

```bash
./build/server/dbms_http_server \
  --data-root ./test_data \
  --host 127.0.0.1 \
  --port 18080
```

Проверка healthcheck:

```bash
curl http://127.0.0.1:18080/health
```

Ожидаемый ответ:

```json
[{"status":"ok"}]
```

### 2. Запустить клиент

```bash
./build/client/dbms_client
```

В интерактивном режиме появится приглашение:

```text
dbms>
```

Теперь можно вводить SQL-команды. Команда считается завершенной, когда заканчивается точкой с запятой.

### 3. Выполнить демо SQL

```sql
CREATE DATABASE standard_demo;
USE standard_demo;

CREATE TABLE users (
  id INT INDEXED,
  name STRING,
  age INT,
  city STRING
);

INSERT INTO users (id, name, age, city) VALUES
  (1, "Ann", 20, "Moscow"),
  (2, "Bob", 30, "Kazan"),
  (3, "Kate", 22, "Moscow");

SELECT * FROM users;
SELECT id AS user_id, name AS user_name FROM users WHERE id == 2;
SELECT name AS user_name, age AS user_age FROM users WHERE id >= 2 ORDER BY id;

UPDATE users SET age = 31, name = "Robert" WHERE id == 2;
SELECT id, name, age FROM users WHERE id == 2;

SELECT COUNT(*) AS total FROM users;
SELECT SUM(age) AS age_sum FROM users;
SELECT AVG(age) AS avg_age FROM users WHERE id == 1;
SELECT city, COUNT(*) AS users_count FROM users GROUP BY city ORDER BY city;

SELECT id AS matched_id FROM users WHERE age >= 20 AND city LIKE "Mos%";
DELETE FROM users WHERE id == 1;
SELECT * FROM users ORDER BY id;

REVERT users "2000.01.01-00:00:00.000";
SELECT * FROM users;
```

### 4. Запустить SQL-скрипт через клиент

```bash
./build/client/dbms_client tests/sql/coursework_full.sql
```

Клиент сам разобьет файл на команды по `;` с учетом строковых литералов и отправит их на сервер.

## Конфигурация клиента

`dbms_client` использует переменные окружения:

| Переменная | Назначение | Значение по умолчанию |
|---|---|---|
| `DBMS_CLIENT_HOST` | host HTTP-сервера | `127.0.0.1` |
| `DBMS_CLIENT_PORT` | port HTTP-сервера | `18080` |
| `DBMS_TOKEN` | JWT-токен для `Authorization: Bearer ...` | пусто |

Пример:

```bash
export DBMS_CLIENT_HOST=127.0.0.1
export DBMS_CLIENT_PORT=18080
export DBMS_TOKEN="<jwt-token>"
./build/client/dbms_client
```

## Конфигурация HTTP-сервера

```text
dbms_http_server [options]
```

Основные параметры:

| Параметр | Назначение | По умолчанию |
|---|---|---|
| `--data-root <path>` | директория файлов БД | `./db_data` |
| `--host <addr>` | адрес привязки | `0.0.0.0` |
| `--port <num>` | HTTP-порт | `18080` |
| `--jwt-secret <secret>` | секрет подписи JWT | `dev-secret` |
| `--cluster-shards <num>` | число in-process shard-узлов | `1` |
| `--async-workers <num>` | число потоков async queue | `2` |
| `--require-auth` | включить обязательную авторизацию | выключено |
| `--storage-node <host:port>` | внешний storage node; можно указать несколько раз | нет |
| `--heartbeat <ms>` | интервал heartbeat | `5000` |
| `--storage-server-path <path>` | путь к storage server для автоперезапуска | пусто |
| `--accounts-file <path>` | JSON-файл аккаунтов и прав | `./accounts.json` |

Также поддерживаются переменные окружения:

| Переменная | Назначение |
|---|---|
| `DBMS_JWT_SECRET` | переопределяет JWT secret |
| `DBMS_REQUIRE_AUTH=1` | включает обязательную авторизацию |

## HTTP API

Все ответы возвращаются как JSON. Для защищенных endpoint при запуске с `--require-auth` нужен заголовок:

```http
Authorization: Bearer <token>
```

### Healthcheck

```http
GET /health
```

Ответ:

```json
[{"status":"ok"}]
```

### Логин

```http
POST /login
Content-Type: application/json

{
  "login": "admin",
  "password": "admin"
}
```

Ответ содержит токен:

```json
[
  {
    "token": "...",
    "username": "admin",
    "is_admin": true,
    "groups": []
  }
]
```

### Синхронный SQL-запрос

```http
POST /query
Content-Type: application/json
Authorization: Bearer <token>

{
  "query": "SELECT * FROM users;",
  "database": "standard_demo"
}
```

Поле `database` особенно важно при включенном RBAC: по нему сервер понимает, права к какой базе надо проверять.

### Асинхронный SQL-запрос

```http
POST /query_async
Content-Type: application/json
Authorization: Bearer <token>

{
  "query": "SELECT SUM(age) AS age_sum FROM users;",
  "database": "standard_demo"
}
```

Пример ответа:

```json
[
  {
    "request_id": "...",
    "status": "queued"
  }
]
```

Проверка статуса:

```http
GET /async/<request_id>
Authorization: Bearer <token>
```

Статусы: `queued`, `running`, `completed`, `failed`.

### Кластер и телеметрия

| Метод | Endpoint | Описание |
|---|---|---|
| `GET` | `/cluster/nodes` | список shard/storage nodes |
| `GET` | `/cluster/stats` | RPS, ошибки, среднее время ответа, режим работы |
| `GET` | `/cluster/heartbeat` | статистика heartbeat-проверок |
| `POST` | `/cluster/add_node` | добавить внешний node |
| `POST` | `/cluster/remove_node` | удалить внешний node |

Добавление узла:

```json
{
  "node_id": "node_1",
  "host": "127.0.0.1",
  "port": 9001
}
```

Удаление узла:

```json
{
  "node_id": "node_1"
}
```

### Аккаунты и RBAC

| Метод | Endpoint | Описание |
|---|---|---|
| `POST` | `/accounts/create` | создать пользователя |
| `GET` | `/accounts` | список пользователей без паролей |
| `POST` | `/accounts/groups/add` | добавить пользователя в группу |
| `POST` | `/accounts/groups/remove` | удалить пользователя из группы |
| `POST` | `/permissions/set` | назначить права |
| `GET` | `/permissions/<database>` | посмотреть права базы |

Создание пользователя:

```json
{
  "username": "reader",
  "password": "reader",
  "is_admin": false
}
```

Назначение прав пользователю:

```json
{
  "database": "standard_demo",
  "target_type": "user",
  "target_name": "reader",
  "permissions": {
    "read": true,
    "write": false,
    "create_table": false,
    "drop_table": false,
    "drop_database": false
  }
}
```

Поддерживаемые операции RBAC:

| Операция | SQL-команды |
|---|---|
| `read` | `SELECT` |
| `write` | `INSERT`, `UPDATE`, `DELETE` |
| `create_table` | `CREATE TABLE` |
| `drop_table` | `DROP TABLE` |
| `drop_database` | `DROP DATABASE` |

Администратор имеет доступ ко всем операциям.

## Запуск с авторизацией

```bash
./build/server/dbms_http_server \
  --data-root ./test_data \
  --host 127.0.0.1 \
  --port 18080 \
  --cluster-shards 2 \
  --require-auth \
  --accounts-file ./accounts.json
```

Получение токена через `curl`:

```bash
TOKEN=$(curl -s -X POST http://127.0.0.1:18080/login \
  -H "Content-Type: application/json" \
  -d '{"login":"admin","password":"admin"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["token"])')
```

Запрос с токеном:

```bash
curl -X POST http://127.0.0.1:18080/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query":"CREATE DATABASE secure_demo;","database":"secure_demo"}'
```

PowerShell-вариант:

```powershell
$login = Invoke-RestMethod `
  -Uri "http://127.0.0.1:18080/login" `
  -Method Post `
  -ContentType "application/json" `
  -Body '{"login":"admin","password":"admin"}'

$token = $login.token
$headers = @{ Authorization = "Bearer $token" }

Invoke-RestMethod `
  -Uri "http://127.0.0.1:18080/query" `
  -Method Post `
  -ContentType "application/json" `
  -Headers $headers `
  -Body '{"query":"SELECT COUNT(*) AS total FROM users;","database":"standard_demo"}'
```

## SQL: поддерживаемый синтаксис

### Базы данных

```sql
CREATE DATABASE db_name;
USE db_name;
DROP DATABASE db_name;
```

### Таблицы

```sql
CREATE TABLE users (
  id INT INDEXED,
  name STRING NOT NULL,
  age INT DEFAULT 18,
  email STRING UNIQUE,
  seq INT AUTO_INCREMENT
);

DROP TABLE users;
```

Типы колонок:

- `INT`;
- `STRING` / `TEXT`.

Модификаторы колонок:

- `NOT NULL`;
- `INDEXED`;
- `UNIQUE`;
- `DEFAULT <literal>`;
- `AUTO_INCREMENT` только для `INT`.

### Insert

```sql
INSERT INTO users (id, name, age) VALUES (1, "Ann", 20);

INSERT INTO users (id, name, age) VALUES
  (1, "Ann", 20),
  (2, "Bob", 30);
```

### Select

```sql
SELECT * FROM users;
SELECT id, name FROM users WHERE id == 1;
SELECT id AS user_id, name AS user_name FROM users WHERE age >= 18 ORDER BY id DESC;
```

Поддерживаются агрегаты:

```sql
SELECT COUNT(*) AS total FROM users;
SELECT SUM(age) AS age_sum FROM users;
SELECT MIN(age) AS min_age FROM users;
SELECT MAX(age) AS max_age FROM users;
SELECT AVG(age) AS avg_age FROM users;
SELECT city, COUNT(*) AS users_count FROM users GROUP BY city ORDER BY city;
```

### Where

```sql
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id == 1;
SELECT * FROM users WHERE age != 18;
SELECT * FROM users WHERE age > 18;
SELECT * FROM users WHERE age >= 18;
SELECT * FROM users WHERE age < 65;
SELECT * FROM users WHERE age <= 65;
SELECT * FROM users WHERE age BETWEEN 18 AND 30;
SELECT * FROM users WHERE city LIKE "Mos%";
SELECT * FROM users WHERE age >= 18 AND city LIKE "Mos%";
SELECT * FROM users WHERE age < 18 OR age > 65;
```

### Update и delete

```sql
UPDATE users SET age = 31, name = "Robert" WHERE id == 2;
DELETE FROM users WHERE id == 1;
```

### Revert

```sql
REVERT users "2000.01.01-00:00:00.000";
```

`REVERT` откатывает состояние таблицы к указанному timestamp на основе журналирования изменений.

## Storage engine

Storage engine отвечает за физическое хранение данных и выполнение `QueryPlan`.

Ключевые компоненты:

- `Engine` — главный класс выполнения операций.
- `BinaryFileManager` — работа с бинарными файлами таблиц.
- `SchemaManager` — хранение и загрузка схем таблиц.
- `BPlusTree` — индексная структура для ускорения поиска.
- `QueryOptimizer` — анализ `WHERE` и выбор стратегии выполнения.
- `WriteAheadLog` — WAL для восстановления после сбоя.
- `Journal` — история изменений для `REVERT`.
- `StringPool` — управление строковыми значениями.

Для индексированных колонок оптимизатор может выбрать:

- `INDEX_SEEK` для точечного поиска;
- `INDEX_RANGE_SCAN` для диапазонов;
- `FULL_TABLE_SCAN`, если подходящего индекса нет.

## Кластерный режим

### In-process shards

Самый простой способ включить шардинг:

```bash
./build/server/dbms_http_server \
  --data-root ./cluster_data \
  --host 127.0.0.1 \
  --port 18080 \
  --cluster-shards 2
```

Проверка:

```bash
curl http://127.0.0.1:18080/cluster/nodes
curl http://127.0.0.1:18080/cluster/stats
```

Если `--cluster-shards 2`, сервер создаст два внутренних узла `shard_0` и `shard_1`.

### External storage nodes

Можно запустить отдельные TCP storage-серверы:

```bash
./build/storage_server --data-dir ./node1_data --host 127.0.0.1 --port 9001
./build/storage_server --data-dir ./node2_data --host 127.0.0.1 --port 9002
```

Затем запустить HTTP-сервер с внешними узлами:

```bash
./build/server/dbms_http_server \
  --data-root ./cluster_data \
  --host 127.0.0.1 \
  --port 18080 \
  --storage-node 127.0.0.1:9001 \
  --storage-node 127.0.0.1:9002
```

Либо добавить узел на лету через API:

```bash
curl -X POST http://127.0.0.1:18080/cluster/add_node \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node_1","host":"127.0.0.1","port":9001}'
```

### Как выбирается shard

- Глобальные команды (`CREATE DATABASE`, `DROP DATABASE`, `USE`, `REVERT`) считаются broadcast/administrative-командами.
- Для табличных запросов сервер пытается извлечь имя таблицы из SQL.
- Имя таблицы хэшируется и через consistent hashing сопоставляется с узлом.
- Heartbeat периодически проверяет доступность внешних nodes.

## Тестирование

### Regression-тест базового SQL

PowerShell:

```powershell
cd dbms_project
./tests/run_regression.ps1
```

Скрипт запускает сервер, создает временную БД, выполняет SQL-команды и проверяет JSON-ответы.

### Advanced regression

```powershell
cd dbms_project
./tests/run_advanced_regression.ps1
```

Проверяются:

- авторизация;
- создание аккаунта;
- RBAC read-only пользователь;
- запрет write-операции для reader;
- async query;
- cluster nodes;
- cluster stats.

### Тест BinaryFileManager

```bash
ctest --test-dir build
```

Или напрямую:

```bash
./build/test_binary_file_manager
```

## Логи и данные

При запуске создаются рабочие файлы:

| Файл / папка | Назначение |
|---|---|
| `./db_data` или `--data-root` | данные БД и таблиц |
| `server_access.log` | access log HTTP-сервера |
| `storage_access.log` | access log storage server |
| `accounts.json` | аккаунты, группы и RBAC-права |
| WAL-файлы внутри data root | журнал восстановления |

Не рекомендуется коммитить runtime-данные, временные базы и логи в репозиторий.

## Типичный сценарий демонстрации

1. Запустить сервер:

```bash
./build/server/dbms_http_server --data-root ./test_data --host 127.0.0.1 --port 18080 --cluster-shards 2 --require-auth
```

2. Получить JWT для `admin`.
3. Создать БД и таблицу.
4. Вставить несколько строк.
5. Выполнить `SELECT`, `WHERE`, `ORDER BY`, агрегаты и `GROUP BY`.
6. Показать `/cluster/nodes` и `/cluster/stats`.
7. Выполнить `/query_async` и получить результат через `/async/<guid>`.
8. Создать пользователя `reader`, выдать ему только `read`, показать, что `SELECT` разрешен, а `UPDATE` возвращает `403`.
9. Показать `server_access.log`.

## Ограничения

Проект является учебной СУБД и не претендует на совместимость с промышленными SQL-серверами.

Из текущей грамматики следуют ограничения:

- нет `JOIN`;
- нет вложенных запросов;
- нет `ALTER TABLE`;
- нет SQL-транзакций как пользовательского синтаксиса;
- нет `LIMIT/OFFSET`;
- нет `HAVING`;
- нет внешних ключей;
- строковые литералы используются в двойных кавычках;
- типы данных ограничены `INT` и `STRING/TEXT`.

## Troubleshooting

### Сервер не стартует

Проверьте, что порт свободен:

```bash
lsof -i :18080
```

Или запустите на другом порту:

```bash
./build/server/dbms_http_server --port 18081
```

### Клиент пишет `Connection failed`

Проверьте:

- запущен ли `dbms_http_server`;
- совпадают ли host и port;
- выставлены ли `DBMS_CLIENT_HOST` и `DBMS_CLIENT_PORT`;
- не блокирует ли соединение firewall.

### Запрос возвращает `missing_query_field`

Для `/query` и `/query_async` тело должно быть JSON-объектом с полем `query`:

```json
{
  "query": "SELECT * FROM users;"
}
```

### Запрос возвращает `forbidden_by_rbac`

Сервер запущен с `--require-auth`, и у пользователя нет прав на выбранную БД. Передайте корректное поле `database` и настройте права через `/permissions/set`.

### Сборка не находит Flex/Bison

Установите зависимости.

Ubuntu/Debian:

```bash
sudo apt update
sudo apt install flex bison cmake g++
```

Windows:

- используйте MSYS2/MinGW с пакетами `flex`, `bison`, `cmake`, `mingw-w64-x86_64-gcc`;
- либо настройте пути к Flex/Bison в CLion.

### Сборка не находит Crow/json/httplib/Boost

Проверьте папку `third_party` и include paths в `CMakeLists.txt`.

## Безопасность

- `dev-secret` подходит только для локальной разработки.
- При запуске с `--require-auth` всегда задавайте собственный `--jwt-secret` или `DBMS_JWT_SECRET`.
- Файл `accounts.json` содержит хэши паролей и соли, поэтому его не стоит публиковать.
- По умолчанию создается администратор `admin` с паролем `admin`; для реального использования пароль нужно заменить.
