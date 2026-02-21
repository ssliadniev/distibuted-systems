## 🛠️ Quick Start
This is the recommended way to run the project.

Run Postgres DB Benchmark
This runs the server using File System storage.
 ```bash
 # Linux / macOS
 STORAGE_TYPE=postgres docker compose up -d --build
 
 # Windows (PowerShell)
 $Env:STORAGE_TYPE="postgres"; docker compose up -d --build
 ```
   
Note: The benchmark will run automatically once the server is healthy. Check the logs of the benchmark container for results.