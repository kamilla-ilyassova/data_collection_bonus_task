import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
from kafka import KafkaProducer

# 1) Scraping

URL = "https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php"

def scrape_all_time_raw():
    print("Downloading page...")
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(URL, headers=headers, timeout=20)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    table = soup.find("table")
    if table is None:
        raise ValueError("No table found on the page")

    rows = table.find_all("tr")
    data = []

    for row in rows[1:]:  
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        player = cols[0].get_text(" ", strip=True)
        team = cols[1].get_text(" ", strip=True)
        goals = cols[2].get_text(" ", strip=True)
        data.append({"player": player, "team": team, "goals": goals})

    df = pd.DataFrame(data)
    print(f"Scraped {len(df)} rows")
    return df

# 2) Cleaning

def clean_data(df):
    cleaned = df.copy()

    # 1. Strip whitespace and lowercase player/team
    cleaned["player"] = cleaned["player"].str.strip()
    cleaned["player"] = cleaned["player"].str.lower()
    cleaned["team"] = cleaned["team"].str.strip()
    cleaned["team"] = cleaned["team"].str.lower()

    # 2. Remove footnotes like [a], [1]
    cleaned["player"] = cleaned["player"].str.replace(r"\[.*?\]", "", regex=True)
    cleaned["team"] = cleaned["team"].str.replace(r"\[.*?\]", "", regex=True)

    # 3. Convert goals to int
    cleaned["goals"] = cleaned["goals"].str.extract(r"(\d+)").astype(int)

    # 4. Remove rows with missing or invalid values
    cleaned = cleaned.dropna(subset=["player", "team", "goals"])

    # 5. Remove duplicates if any
    cleaned = cleaned.drop_duplicates()

    return cleaned

# 3) Kafka producer

def produce_to_kafka(df, topic_name="bonus_23B151041"):  
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic_name, value=message)
    producer.flush()
    print(f"Produced {len(df)} messages to Kafka topic '{topic_name}'")

# 4) Main

def main():
    raw_df = scrape_all_time_raw()
    cleaned_df = clean_data(raw_df)

    cleaned_df.to_csv("cleaned_data.csv", index=False)
    print("Saved cleaned_data.csv")

    produce_to_kafka(cleaned_df, topic_name="bonus_12345678")

    print("\nSample cleaned data:")
    print(cleaned_df.head())

if __name__ == "__main__":
    main()
