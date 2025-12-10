# ETL Pipeline – CSV to PostgreSQL with Modular Design

A C#-based ETL (Extract, Transform, Load) pipeline that reads data from CSV files, applies conditional transformations, and loads into a PostgreSQL database.  
Built with clean architectural principles and multiple design patterns for extensibility and maintainability. Includes unit tests with **xUnit** and **FluentAssertions** for reliability.

---

## 🚀 Features
- **CSV to PostgreSQL conversion** with schema mapping  
- **Conditional transformation engine** using Composition pattern  
- **Aggregation logic** powered by Strategy pattern  
- **Adapter pattern** for flexible data source integration  
- **Factory pattern** for dynamic component instantiation  
- **Unit tests** written with xUnit and FluentAssertions  
- **Modular ETL stages** for clean separation of concerns  

---

## 🏗️ Architecture Overview
- **Extract Layer**
  - Reads CSV files and parses raw data
  - Uses Adapter + Factory to support multiple formats

- **Transform Layer**
  - Applies conditional logic via Composition pattern
  - Aggregates data using Strategy pattern

- **Load Layer**
  - Inserts transformed data into PostgreSQL
  - Handles batch inserts and schema validation

- **Testing Layer**
  - Unit tests with xUnit
  - FluentAssertions for expressive, readable test validation

---

## 🛠️ Technologies
- C# (.NET Core)  
- PostgreSQL  
- CSV file parsing  
- Adapter, Factory, Strategy, and Composition design patterns  
- xUnit + FluentAssertions for unit testing  
