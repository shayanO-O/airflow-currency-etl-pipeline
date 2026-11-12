# Currency Exchange Rates ETL Pipeline

## Project Overview
This project builds an automated ETL pipeline that pulls daily currency exchange rates from the Frankfurter API and stores them in MongoDB. The workflow is managed by Apache Airflow through Astro CLI.

## Main Features
- Extracts daily exchange rates for EUR to USD, GBP and JPY from the public Frankfurter API
- Transforms the API response into clean documents with consistent keys and timestamps
- Passes data across pipeline stages using Airflow named XCom keys
- Loads transformed data into MongoDB with safe upserts based on currency pair and rate date
- Provides Mongo Express as a browser interface to view database records

## Technologies Used
- Apache Airflow 3
- Astro CLI
- Docker and Docker Compose
- MongoDB 7
- Mongo Express
- Python 3 with requests and pymongo

## Running Instructions
1. Make sure Docker Desktop is running on your machine.
2. Create a local `.env` file in the project root with the following variables:
   ```env
   MONGO_INITDB_ROOT_USERNAME=admin
   MONGO_INITDB_ROOT_PASSWORD=<your_mongo_password>
   MONGO_HOST=mongodb
   MONGO_PORT=27017
   MONGO_DB=currency_exchange
   MONGO_COLLECTION=exchange_rates
   ME_CONFIG_MONGODB_URL=mongodb://admin:<your_mongo_password>@mongodb:27017/
   ME_CONFIG_MONGODB_ADMINUSERNAME=admin
   ME_CONFIG_MONGODB_ADMINPASSWORD=<your_mongo_password>
   ME_CONFIG_BASICAUTH_USERNAME=admin
   ME_CONFIG_BASICAUTH_PASSWORD=<your_mongo_express_password>
   ```
3. Start the entire environment using Astro CLI:
   ```bash
   astro dev start
   ```
4. Access the web interfaces in your browser:
   - Airflow UI: http://localhost:8080
   - Mongo Express: http://localhost:8081
5. To stop the environment, run:
   ```bash
   astro dev stop
   ```

## Important Notes
- The `.env` file contains local credentials and is ignored by Git.
- MongoDB data persists across container restarts through the `mongo_data` Docker volume.
