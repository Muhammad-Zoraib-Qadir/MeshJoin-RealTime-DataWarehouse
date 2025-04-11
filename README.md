# MESHJOIN Real-Time Data Warehouse for Metro (Cash and Carry)

![Java](https://img.shields.io/badge/Java-17%2B-orange)
![MySQL](https://img.shields.io/badge/MySQL-8.0-blue)
![License](https://img.shields.io/badge/License-MIT-green)

A near real-time data warehouse solution leveraging the **MESHJOIN algorithm** to optimize ETL performance for Metro (Cash and Carry). Features a star schema design, OLAP queries, and Java-based ETL processes.

## Features
- **MESHJOIN Algorithm**: Efficiently handles streaming data for near real-time updates.
- **Star Schema Design**: Optimized for OLAP querying and reporting.
- **ETL Pipeline**: Java-based data extraction, transformation, and loading.
- **OLAP Queries**: Pre-built analytical queries for business insights.

## Prerequisites
- **Java 17+**
- **MySQL Workbench 8.0+**
- **Eclipse IDE (for Java Developers)**
- **MySQL Connector/J** (JDBC driver `.jar` file)

## Setup & Execution

### 1. Database Setup
- Open **MySQL Workbench** and run [`Star_Schema_Creation.sql`](Star_Schema_Creation.sql) to create tables and grant permissions to user `Admin`.

### 2. Configure Eclipse Project
1. Open Eclipse and import the **ETL_Java_Project** folder.
2. Right-click the project → **Build Path** → **Configure Build Path** → **Libraries** → **Add JARs**.
3. Add the `mysql-connector-java-*.jar` file (download it [here](https://dev.mysql.com/downloads/connector/j/) if missing).

### 3. Run the ETL Pipeline
1. Open [`Main.java`](ETL_Java_Project/src/Main.java) in Eclipse.
2. Right-click → **Run As** → **Java Application**.
3. Enter credentials when prompted:
User: Admin
Password: datawarehouse

## OLAP Querying
Run [`OLAP_Queries.sql`](OLAP_Queries.sql) in MySQL Workbench to execute predefined analytical queries on the data warehouse.

## Notes
- 🔑 Ensure the MySQL user `Admin` has full privileges on the database.
- ⚠️ Use the **exact same credentials** (`Admin`/`datawarehouse`) to avoid connection errors.

## License
This project is licensed under the [MIT License](LICENSE).
## Why This Works

### 🧠 Clarity  
Step-by-step instructions with embedded code snippets and **highlighted credentials** ensure users can follow along without confusion.

### 🏗️ Structure  
Visual project layout breakdown separates SQL schema scripts from Java ETL code, making navigation intuitive.

### 🛡️ Badges  
Shields.io badges (Java 17+, MySQL 8.0) signal compatibility and tooling at a glance.

### 🔧 Focus on Pain Points  
Explicit warnings (e.g., credential format, JAR file setup) prevent common pitfalls during installation.
