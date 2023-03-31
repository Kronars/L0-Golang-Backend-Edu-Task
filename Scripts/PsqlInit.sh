#!/bin/bash

user_name=go_client
user_pass=go_passwd
db_name=wb_l0

# Проверяем, установлен ли PostgreSQL
if ! command -v psql &> /dev/null
then
    echo "PostgreSQL не установлен."
    exit 1
fi

sudo -u postgres psql -c "CREATE USER $user_name WITH PASSWORD '$user_pass'"

if [ $? -eq 0 ]
then
    echo "[Init] Пользователь '$user_name' успешно создан."
else
    echo "Ошибка при создании пользователя."
    exit 1
fi

# Создаем базу данных
sudo -u postgres psql -c "CREATE DATABASE $db_name ENCODING = 'UTF8'" 

# Проверяем результат
if [ $? -eq 0 ]
then
    echo "[Init] База данных '$db_name' успешно создана."
else
    echo "Ошибка при создании базы данных."
    exit 1
fi

# sudo -u postgres dropdb $db_name
# sudo -u postgres psql -c "DROP ROLE $user_name;"

exit 0
