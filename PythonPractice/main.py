from db import connect_to_db, create_table, insert_data, select_by_ip, select_by_date, select_by_range
import json

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

connection = None
connection = connect_to_db(config_data)
create_table = create_table(connection)
insert_data = insert_data(connection, config_data)

while True:
    answer = input('Что вы хотите сделать?\n1 - Просмотр данных из БД (групировка по IP)\n2 - Просмотр данных из БД (групировка по дате)\n3 - Просмотр данных из БД (выборка по промежутку дат)\n4 - Запуск API\n5 - Выход\nОтвет: ')
    if answer == '1':
        ip = input('Введите IP адрес: ')
        select_by_ip = select_by_ip(connection, ip)
    elif answer == '2':
        date = input('Введите дату: ')
        select_by_date = select_by_date(connection, date)
    elif answer == '3':
        date_start = input('Введите начальную дату: ')
        date_end = input('Введите конечную дату: ')
        select_by_range = select_by_range(connection, date_start, date_end)
    elif answer == '4':
        print('Объяснение находится в файле README.md')
    elif answer == '5':
        connection.close()
        break
    else:
        print('Введите корректный ответ')