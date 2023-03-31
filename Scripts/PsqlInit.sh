#!/bin/bash

# Проверяем, установлен ли PostgreSQL
if ! command -v psql &> /dev/null
then
    echo "PostgreSQL не установлен."
    exit 1
fi

# Создаем базу данных
sudo -u postgres psql -c "CREATE DATABASE WB_L0 ENCODING = 'UTF8'" 

# Проверяем результат
if [ $? -eq 0 ]
then
    echo "База данных 'WB_L0' успешно создана."
    exit 0
else
    echo "Ошибка при создании базы данных."
    exit 1
fi