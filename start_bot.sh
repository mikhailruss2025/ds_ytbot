#!/bin/bash

echo "Запуск Discord Music Bot..."
echo

# Проверяем наличие Python
if ! command -v python3 &> /dev/null; then
    echo "Python не установлен! Пожалуйста, установите Python 3.8 или выше."
    exit 1
fi

# Проверяем наличие FFmpeg
if ! command -v ffmpeg &> /dev/null; then
    echo "FFmpeg не установлен! Пожалуйста, установите FFmpeg."
    exit 1
fi

# Проверяем наличие файла .env
if [ ! -f .env ]; then
    echo "Файл .env не найден! Создаем новый..."
    echo "DISCORD_TOKEN=your_token_here" > .env
    echo "Пожалуйста, отредактируйте файл .env и добавьте ваш токен Discord."
    exit 1
fi

# Создаем виртуальное окружение, если его нет
if [ ! -d "venv" ]; then
    echo "Создание виртуального окружения..."
    python3 -m venv venv
fi

# Активируем виртуальное окружение
source venv/bin/activate

# Устанавливаем зависимости
echo "Установка зависимостей..."
pip install -r requirements.txt

# Функция для запуска бота
run_bot() {
    python bot.py
}

# Запускаем бота
echo "Запуск бота..."
while true; do
    run_bot
    echo
    echo "Бот остановлен. Перезапуск через 5 секунд..."
    sleep 5
done 