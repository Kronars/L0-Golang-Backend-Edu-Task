#!/bin/bash

tables=$(sudo -u postgres psql -tAc "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")

users=$(sudo -u postgres psql -tAc "SELECT user FROM pg_catalog.pg_user;")

# Выводим результаты в консоль
echo "Созданные таблицы:"
echo $tables
echo "Существующие пользователи: $users"
