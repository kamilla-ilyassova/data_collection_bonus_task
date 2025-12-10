import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
from kafka import KafkaProducer

# 1) Scraping all tables

URL = "https://www.thesoccerworldcups.com/statistics/all_time_top_scorers.php"

def scrape_all_time_raw():
    print("Downloading page...")
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(URL, headers=headers, timeout=20)
    res.raise_for_status()
    soup = BeautifulSoup(res.text, "html.parser")

    tables = soup.find_all("table")
    if not tables:
        raise ValueError("No tables found on the page")

    dfs = []

    for table in tables:
        rows = table.find_all("tr")
        data = []
        for row in rows[1:]:  
            cols = row.find_all("td")
            if len(cols) < 4:
                continue
            rank = cols[0].get_text(strip=True)
            player = cols[1].get_text(strip=True)
            goals = cols[2].get_text(strip=True)
            team = cols[-1].get_text(strip=True)  

            data.append({
                "rank": rank,
                "player": player,
                "goals": goals,
                "team": team
            })
        if data:
            dfs.append(pd.DataFrame(data))

    df = pd.concat(dfs, ignore_index=True)

    df["rank"] = df["rank"].replace("", pd.NA)
    df["rank"] = df["rank"].ffill()
    df["rank"] = df["rank"].str.replace(".", "", regex=False).astype(int)

    print(f"Scraped {len(df)} rows from {len(dfs)} tables")
    return df

# 2) Cleaning

def clean_data(df):
    cleaned = df.copy()

    # 1. Strip whitespace and lowercase player/team
    cleaned["player"] = cleaned["player"].str.strip().str.lower()
    cleaned["team"] = cleaned["team"].str.strip().str.lower()

    # 2. Remove footnotes like [a], [1]
    cleaned["player"] = cleaned["player"].str.replace(r"\[.*?\]", "", regex=True)
    cleaned["team"] = cleaned["team"].str.replace(r"\[.*?\]", "", regex=True)

    # 3. Convert goals to int
    cleaned["goals"] = cleaned["goals"].str.extract(r"(\d+)").astype(int)

    # 4. Remove rows with missing values
    cleaned = cleaned.dropna(subset=["player", "team", "goals"])

    # 5. Remove duplicates
    cleaned = cleaned.drop_duplicates()

    # 6. Reorder columns
    cleaned = cleaned[["rank", "player", "goals", "team"]]

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
        print(f"Produced message to Kafka: {message}")  

    producer.flush()
    print(f"Total {len(df)} messages produced to Kafka topic '{topic_name}'")

# 4) Main

def main():
    raw_df = scrape_all_time_raw()
    cleaned_df = clean_data(raw_df)

    cleaned_df.to_csv("cleaned_data.csv", index=False)
    print("Saved cleaned_data.csv")

    produce_to_kafka(cleaned_df, topic_name="bonus_23B151041")

if __name__ == "__main__":
    main()
