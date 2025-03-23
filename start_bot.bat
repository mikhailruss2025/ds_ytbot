@echo off
echo Запуск Discord Music Bot...
echo.

REM Проверяем наличие Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python не установлен! Пожалуйста, установите Python 3.8 или выше.
    pause
    exit /b 1
)

REM Проверяем наличие FFmpeg
ffmpeg -version >nul 2>&1
if errorlevel 1 (
    echo FFmpeg не установлен! Пожалуйста, установите FFmpeg.
    pause
    exit /b 1
)

REM Проверяем наличие файла .env
if not exist .env (
    echo Файл .env не найден! Создаем новый...
    echo DISCORD_TOKEN=your_token_here > .env
    echo Пожалуйста, отредактируйте файл .env и добавьте ваш токен Discord.
    pause
    exit /b 1
)

REM Устанавливаем зависимости
echo Установка зависимостей...
pip install -r requirements.txt

REM Запускаем бота
echo Запуск бота...
python bot.py

REM Если бот упал, ждем 5 секунд и перезапускаем
:restart
echo.
echo Бот остановлен. Перезапуск через 5 секунд...
timeout /t 5 /nobreak
python bot.py
goto restart 