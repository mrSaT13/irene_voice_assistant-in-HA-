# Irene Voice Assistant Integration for Home Assistant

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-orange.svg)](https://github.com/custom-components/hacs)
[![GitHub release](https://img.shields.io/github/release/mrSaT13/irene_voice_assistant-in-HA-.svg)](https://GitHub.com/mrSaT13/irene_voice_assistant-in-HA-/releases/)

🇷🇺 Профессиональная интеграция голосового ассистента [Irene](https://github.com/AlexeyBond/Irene-Voice-Assistant) с Home Assistant

## 🚀 Возможности

✅ **Полная интеграция с умным домом** - управляйте устройствами голосом через Ирину  
✅ **Текстовый чат** - общайтесь через красивый веб-интерфейс  
✅ **Голосовое управление** - говорите команды через микрофон  
✅ **История диалогов** - полная история всех сообщений  
✅ **TTS (Text-to-Speech)** - озвучка ответов и уведомлений  
✅ **WebSocket** - быстрое двустороннее соединение в реальном времени  
✅ **REST API** - HTTP endpoints для интеграции  
✅ **Настройки через UI** - удобная конфигурация без YAML  
✅ **HACS поддержка** - легкая установка через магазин  
✅ **Автоматизации** - используйте в автоматизациях Home Assistant  



<img width="573" height="560" alt="изображение" src="https://github.com/user-attachments/assets/941903f7-9647-4707-bb9b-9b2d7ccff10a" />

## 📦 Установка

### Через HACS (рекомендуется)

1. Откройте HACS в Home Assistant
2. Нажмите на три точки в правом верхнем углу → "Пользовательские репозитории"
3. Добавьте репозиторий:
   - **URL:** `https://github.com/mrSaT13/irene_voice_assistant-in-HA-`
   - **Категория:** Integration
4. Найдите "Irene Voice Assistant" в поиске и установите
5. Перезапустите Home Assistant

### Ручная установка

1. Скачайте или клонируйте репозиторий
2. Скопируйте папку `custom_components/irene_voice_assistant` в папку `custom_components` вашего Home Assistant
3. Перезапустите Home Assistant

## ⚙️ Настройка

1. Перейдите в **Настройки** → **Устройства и службы** → **Интеграции**
2. Нажмите **"+ Добавить интеграцию"**
3. Найдите **"Irene Voice Assistant"**
4. Заполните данные:
   - **Host**: IP адрес сервера Irene (например, `192.168.1.100`)
   - **Port**: порт сервера Irene (по умолчанию `5003`)
   - **SSL**: включите если используете HTTPS
   - **Name**: имя ассистента (по умолчанию `Irene`)

### Опции настройки

После добавления можно настроить:
- **Формат ответа**:
  - `none` - TTS на сервере Irene
  - `saytxt` - текстовый ответ
  - `saywav` - WAV аудио (рекомендуется)

## 💬 Использование

### Веб-интерфейс чата

Откройте в браузере:
