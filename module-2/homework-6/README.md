# How to run


### 1. Start the Database:
```bash
docker-compose up -d neo4j
```

### 2. Run first part:
```bash
docker-compose run app python src/main.py --mode queries
```

### 2. Run second part:
```bash
docker compose run app python src/main.py --mode benchmark
```
