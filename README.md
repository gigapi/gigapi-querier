![image](https://github.com/user-attachments/assets/fa3788a2-9a5b-47bf-b6ef-f818ba62a404)

# GigAPI Query Engine

GigAPI Go provides a Flight SQL and HTTP interface to query time-series using GigAPI Catalog Metadata and DuckDB

> [!WARNING]  
> GigAPI is an open beta developed in public. Bugs and changes should be expected. Use at your own risk.
> 

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Quick Start

### Docker
Run `gigapi` _all-in-one_ using Docker making sure the proper `data` with local storage
```yaml
services:
  gigapi:
    image: ghcr.io/gigapi/gigapi:latest
    container_name: gigapi
    hostname: gigapi
    restart: unless-stopped
    volumes:
      - ./data:/data
    ports:
      - "7971:7971"
    environment:
      - GIGAPI_ROOT=/data
      - GIGAPI_LAYERS_0_NAME=default
      - GIGAPI_LAYERS_0_TYPE=fs
      - GIGAPI_LAYERS_0_URL=file:///data

```
Gigapi-querier is part of [gigapi](https://github.com/gigapi/gigapi)

### Build
```bash
# Build from source
go generate
go build -o gq .

# Start the server
PORT=8080 DATA_DIR=./data ./gigapi
```

### Configuration

- `PORT`: Main server port (default: 8080)
- `FLIGHTSQL_PORT`: FlightSQL API server port (default: 8082)
- `DATA_DIR`: Path to data directory (default: ./data)
- `DISABLE_UI`: Disable web UI (optional)

## <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> API Endpoints

### Query Data

#### Query Processing Logic

1. Parse SQL query to extract FROM db.table and time range
2. Find relevant parquet files using catalog metadata
3. Use DuckDB to execute optimized queries against selected files
4. Post-process results to handle BigInt timestamps



#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> API
```bash
$ curl -X POST "http://localhost:8080/query?db=mydb" \
  -H "Content-Type: application/json"  \
  -d '{"query": "SELECT time, location, temperature FROM weather WHERE time >= '2025-04-01T00:00:00'"}'
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> CLI
The GigAPI Querier can also be used in CLI mode to execute an individual query

```bash
$ ./gigapi --query "SELECT count(*), avg(temperature) FROM weather" --db mydb
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> FlightSQL
GigAPI data can be accessed using FlightSQL GRPC clients in any language
```python
from flightsql import connect, FlightSQLClient
client = FlightSQLClient(host='localhost',port=8082,insecure=True,metadata={'bucket':'hep'})
conn = connect(client)
cursor = conn.cursor()
cursor.execute('SELECT 1, version()')
print("rows:", [r for r in cursor])
```

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> UI
A quick and dirty query user-interface is also provided for testing
![image](https://github.com/user-attachments/assets/a9f09b3f-10fc-42e3-9092-770252e0d8d3)

#### <img src="https://github.com/user-attachments/assets/a9aa3ebd-9164-476d-aedf-97b817078350" width=24 /> Grafana
GigAPI can be used from Grafana using the InfluxDB3 Flight GRPC Datasource

![image](https://github.com/user-attachments/assets/a7849ff4-b8f6-433b-8458-1c47394c5e5f)


<br>

### Got Questions?
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/gigapi/gigapi-querier)

## License

<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/AGPLv3_Logo.svg/2560px-AGPLv3_Logo.svg.png" width=200>

> Gigapipe is released under the GNU Affero General Public License v3.0 ©️ HEPVEST BV, All Rights Reserved.
