#!/usr/bin/env python3
"""
DTEK Фактичний Графік Парсер
============================
Использует curl_cffi для обхода защиты Incapsula.
Можно запускать по cron для автоматического обновления данных.

Требования: pip install curl_cffi

Пример cron: 0 8 * * * /usr/bin/python3 /path/to/dtek_fact_parser.py
"""

from curl_cffi import requests
import json
import re
from datetime import datetime
import os

# ==========================================
# ⚙️ НАСТРОЙКИ
# ==========================================
URL = "https://www.dtek-dnem.com.ua/ua/shutdowns"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "dtek_schedule.json")

# Какие группы парсить (можно изменить)
GROUPS_TO_PARSE = ["GPV1.1", "GPV1.2", "GPV2.1", "GPV2.2", "GPV3.1", "GPV3.2",
                   "GPV4.1", "GPV4.2", "GPV5.1", "GPV5.2", "GPV6.1", "GPV6.2"]
# ==========================================


def fetch_dtek_page():
    """Получает HTML страницы DTEK, обходя защиту."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 📡 Подключение к DTEK...")
    
    response = requests.get(URL, impersonate="chrome120", timeout=30)
    
    if response.status_code != 200:
        raise Exception(f"HTTP Error: {response.status_code}")
    
    if len(response.text) < 1000 or "DisconSchedule" not in response.text:
        raise Exception("Получена страница защиты, а не контент сайта")
    
    print(f"✅ Страница получена успешно ({len(response.text)} байт)")
    return response.text


def parse_schedule(html: str) -> dict:
    """Извлекает данные графика из HTML."""
    
    # Парсим DisconSchedule.fact (фактический график)
    # Ищем начало и затем находим конец JSON объекта
    fact_start = html.find('DisconSchedule.fact = {')
    if fact_start == -1:
        raise Exception("Не удалось найти DisconSchedule.fact в HTML")
    
    # Находим начало JSON объекта
    json_start = html.find('{', fact_start)
    
    # Ищем конец объекта, считая скобки
    brace_count = 0
    json_end = json_start
    for i, char in enumerate(html[json_start:], start=json_start):
        if char == '{':
            brace_count += 1
        elif char == '}':
            brace_count -= 1
            if brace_count == 0:
                json_end = i + 1
                break
    
    fact_json_str = html[json_start:json_end]
    fact_data = json.loads(fact_json_str)
    
    # Парсим DisconSchedule.preset (плановый график) - аналогично
    preset_start = html.find('DisconSchedule.preset = {')
    preset_data = {}
    if preset_start != -1:
        json_start = html.find('{', preset_start)
        brace_count = 0
        for i, char in enumerate(html[json_start:], start=json_start):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    preset_data = json.loads(html[json_start:i+1])
                    break
    
    return {
        "fact": fact_data,
        "preset": preset_data,
        "fetched_at": datetime.now().isoformat()
    }


def format_schedule_for_group(fact_data: dict, group: str) -> dict:
    """Форматирует расписание для одной группы."""
    result = {"group": group, "days": {}}
    
    if "data" not in fact_data:
        return result
    
    for timestamp, groups_data in fact_data["data"].items():
        date = datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        
        if group in groups_data:
            hours_data = groups_data[group]
            result["days"][date] = {}
            
            for hour_str, status in hours_data.items():
                hour = int(hour_str)
                time_range = f"{hour-1:02d}:00-{hour:02d}:00"
                
                # Преобразуем статус в читаемый вид
                if status == "yes":
                    readable_status = "light_on"
                elif status == "no":
                    readable_status = "light_off"
                elif status == "first":
                    readable_status = "off_first_30min"
                elif status == "second":
                    readable_status = "off_second_30min"
                elif "maybe" in status:
                    readable_status = "possible_outage"
                else:
                    readable_status = status
                
                result["days"][date][time_range] = readable_status
    
    return result


def main():
    try:
        # 1. Получаем страницу
        html = fetch_dtek_page()
        
        # 2. Парсим данные
        print("🔍 Парсинг данных...")
        raw_data = parse_schedule(html)
        
        # 3. Форматируем для каждой группы
        output = {
            "fetched_at": raw_data["fetched_at"],
            "update_time": raw_data["fact"].get("update", "unknown"),
            "groups": {}
        }
        
        for group in GROUPS_TO_PARSE:
            formatted = format_schedule_for_group(raw_data["fact"], group)
            if formatted["days"]:
                output["groups"][group] = formatted["days"]
        
        # 4. Сохраняем в JSON
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Данные сохранены в: {OUTPUT_FILE}")
        
        # 5. Выводим краткую сводку
        print(f"\n📊 СВОДКА:")
        print(f"   Время обновления на сайте: {output['update_time']}")
        print(f"   Групп с данными: {len(output['groups'])}")
        
        # Показываем сегодняшний график для группы 1.1
        if "GPV1.1" in output["groups"]:
            today = datetime.now().strftime("%Y-%m-%d")
            if today in output["groups"]["GPV1.1"]:
                print(f"\n💡 GPV1.1 на сегодня ({today}):")
                schedule = output["groups"]["GPV1.1"][today]
                
                light_on = [k for k, v in schedule.items() if v == "light_on"]
                light_off = [k for k, v in schedule.items() if v == "light_off"]
                
                if light_on:
                    print(f"   ✅ Світло є: {', '.join(sorted(light_on)[:5])}...")
                if light_off:
                    print(f"   🔴 Світла немає: {', '.join(sorted(light_off)[:5])}...")
        
        print("\n✅ Готово!")
        return True
        
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        return False


if __name__ == "__main__":
    main()
