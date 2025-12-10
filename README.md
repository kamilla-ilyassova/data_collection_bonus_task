# Bonus Task: Web Scraping → Kafka Mini-Pipeline

## Student
**ID:** 23B151041  
**Name:** Kamilla Ilyassova  
**GitHub link:** https://github.com/kamilla-ilyassova/data_collection_bonus_task/tree/main

---

## 1. Project Overview
This project demonstrates a tiny data pipeline that scrapes data from a static webpage, cleans it, and sends each row into a Kafka topic as JSON messages.  

The dataset contains all-time top goalscorers in FIFA World Cups, scraped from [The Soccer World Cups](https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php).

---

## 2. Website / Data
**URL:** [https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php](https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php)  

**Data Provided / Columns:**
- `rank` – Player rank
- `player` – Player name
- `goals` – Number of goals
- `team` – Player national team  

  It is static page, that is easy to scrape with  BeautifulSoup and requests libraries. It contains multiple tables with sufficient rows (>20).

---

## 3. Cleaning Steps
1. Trimmed whitespace from `player` and `team`.  
2. Converted `player` and `team` names to lowercase.  
3. Removed footnotes `[a]`, `[1]`, etc. using regex.  
4. Converted `goals` column to integer.  
5. Dropped rows with missing values.  
6. Removed duplicate rows.  
7. Reordered columns as `rank`, `player`, `goals`, `team`.
At the end of the cleaning stage we get the following data in cleaned_data.csv file.
<img width="495" height="484" alt="2025-12-11_01-39-36" src="https://github.com/user-attachments/assets/1638c5dd-498d-4e07-b3ec-53c6f4325e86" />
<img width="720" height="75" alt="2025-12-11_01-40-05" src="https://github.com/user-attachments/assets/76c67285-87fc-4c5c-b085-6ec6f95d5d34" />

---

## 4. Kafka Pipeline
- **Topic Name:** `bonus_23B151041`  
- **Kafka Producer:** Uses `kafka-python` to send each row as a JSON message.  
- **Bootstrap Server:** `localhost:9092`  

**Example Kafka Message:**
```json
{
  "rank": "1",
  "player": "miroslav klose",
  "goals": 16,
  "team": "germany"
}
```
**Here is additional messages that appeared in the VS Code terminal:**

<img width="1018" height="222" alt="2025-12-11_01-38-41" src="https://github.com/user-attachments/assets/a335044c-6f54-4190-97c3-0c623e712007" />
<img width="1016" height="220" alt="2025-12-11_01-39-10" src="https://github.com/user-attachments/assets/993e70aa-5637-4a74-b4a9-a3123081e4c5" />

