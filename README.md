# Bonus Task: Web Scraping → Kafka Mini-Pipeline

## Student
**ID:** 23B151041  
**Name:** Kamilla Ilyassova  

---

## 1. Project Overview
This project demonstrates a tiny data pipeline that scrapes data from a static webpage, cleans it, and sends each row into a Kafka topic as JSON messages.  

The dataset contains **all-time top goalscorers in FIFA World Cups**, scraped from [The Soccer World Cups](https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php).

---

## 2. Website / Data
**URL:** [https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php](https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php)  

**Data Provided / Columns:**
- `rank` – Player rank
- `player` – Player name
- `goals` – Number of goals
- `team` – Player national team  

**Pros:**
- Static page, easy to scrape without Selenium.
- Contains multiple tables with sufficient rows (>20).

**Cons:**
- Requires cleaning (footnotes like `[a]`, extra spaces, inconsistent case).

---

## 3. Cleaning Steps
1. Trimmed whitespace from `player` and `team`.  
2. Converted `player` and `team` names to lowercase.  
3. Removed footnotes `[a]`, `[1]`, etc. using regex.  
4. Converted `goals` column to integer.  
5. Dropped rows with missing values.  
6. Removed duplicate rows.  
7. Reordered columns as `rank`, `player`, `goals`, `team`.

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
