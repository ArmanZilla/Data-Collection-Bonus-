# BONUS TASK
**Student Name:** Arman Zhilikbay 
**Student ID:** 22B030358 
**Project:** Web Scraping + Kafka Pipeline

## Data Source 
**URL:** https://www.boxofficemojo.com/year/2024/

**URL:** https://www.boxofficemojo.com/year/2024/ 

**Description:** This project scrapes 2024 box office data from Box Office Mojo, which contains comprehensive movie revenue information including:
- Movie rankings 
- Release titles 
- Worldwide gross revenue 
- Domestic gross revenue 
- Foreign gross revenue 
- Number of theaters 
- Release dates

## Data Cleaning Steps
The pipeline performs **7 data cleaning operations** on the scraped data:
### 1. **Rename Columns** 
### 2. **Convert Revenue to Numeric** 
### 3. **Trim Whitespace** 
### 4. **Remove Missing Data** -
### 5. **Convert Rank to Integer** 
### 6. **Add Timestamp** 
### 7. **Remove Empty Rows**

## Sample Kafka Message

  {
    "rank":1,
    "release":"Inside Out 2",
    "gross":652980194,
    "theaters":"4,440",
    "total_gross":652980194,
    "release_date":"Jun 14",
    "distributor":"Walt Disney Studios Motion Pictures",
    "estimated":"false",
    "scraped_at":"2025-12-13T11:51:57.221047"
  }

**Message Format:** 
- **Encoding:** UTF-8 
- **Serialization:** JSON 
- **Topic:** bonus_22B030358 
- **All numeric values** are converted to proper numeric types (int/float) 
- **NaN values** are converted to null for valid JSON

## Setup and Installation ### Prerequisites 
- **Docker Desktop** installed and running 
- **Python 3.8+** installed 
- **pip** package manager

#### 3. Start Kafka with Docker Compose
```bash
docker-compose up -d
```

Wait 20-30 seconds for Kafka to fully initialize. 
#### 4. Verify Kafka is running
```bash
docker-compose ps
```

## Running the Pipeline 
### Execute the script
```bash
py script.py
```

## Verifying Data in Kafka

### Check if topic was created
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### View messages in Kafka (first 5)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic bonus_22B030358 --from-beginning --max-messages 5
```

### Get topic statistics
```bash
docker exec -it kafka kafka-topics --describe --topic bonus_22B030358 --bootstrap-server localhost:9092
```

### Count total messages
```bash
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic bonus_22B030358
```

### Stopping
```bash
docker-compose down
```

