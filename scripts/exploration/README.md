# 📊 Exploration Layer (EDA)

## 📖 Overview

This folder contains SQL scripts used for Exploratory Data Analysis (EDA) on the Gold Layer of the Data Warehouse.

The purpose of this exploration layer is to analyze and understand the business-ready data model before building:
- dashboards,
- reports,
- KPIs,
- or advanced analytical solutions.

The scripts inside this folder help uncover:
- business patterns,
- data distributions,
- customer behavior,
- product performance,
- sales trends,
- and analytical insights.

This stage acts as the bridge between:
- raw warehouse data,
- and business intelligence reporting.

---

# 🎯 Objectives of the Exploration Layer

The main objectives of this exploration process are:

- Understand the structure of analytical datasets
- Validate transformed business data
- Analyze dimensions and measures
- Discover trends and distributions
- Generate business insights
- Support reporting and dashboard development
- Build confidence in the analytical model

---

# 🧠 What is Exploratory Data Analysis (EDA)?

Exploratory Data Analysis (EDA) is the process of investigating, analyzing, and understanding datasets before performing formal reporting or advanced analytics.

EDA helps analysts and engineers:
- understand data behavior,
- identify patterns,
- validate business logic,
- detect anomalies,
- and explore relationships between datasets.

In data warehousing projects, EDA is essential because it ensures that:
- transformed data is trustworthy,
- business metrics are meaningful,
- and analytical models are correctly designed.

---

# 🏗️ Gold Layer Context

The exploration scripts primarily analyze the Gold Layer of the warehouse.

The Gold Layer contains:
- cleaned,
- transformed,
- integrated,
- and analytics-ready datasets.

These tables are optimized for:
- reporting,
- business intelligence,
- KPI analysis,
- and decision-making.

---

# 📂 Exploration Folder Structure

```text
📦 scripts/
│
└── 📁 exploration/
    │
    ├── 📄 README.md
    │
    ├── 📄 database_exploration.sql     # Explore schemas, tables, columns, and metadata
    │
    ├── 📄 dimension_exploration.sql    # Analyze categorical and descriptive business dimensions
    │
    ├── 📄 date_range_exploration.sql   # Analyze historical timelines and date coverage
    │
    ├── 📄 measures_exploration.sql     # Calculate high-level business KPIs and metrics
    │
    ├── 📄 magnitude_analysis.sql       # Compare business measures across dimensions
    │
    └── 📄 ranking_analysis.sql         # Identify top and bottom performers
```

---

# 🗂️ Gold Layer Tables Used

The exploration scripts primarily use the following analytical tables:

| Table Name | Description |
|---|---|
| `gold.dim_customers` | Customer dimension containing customer attributes |
| `gold.dim_products` | Product dimension containing product hierarchy and details |
| `gold.fact_sales` | Sales fact table containing transactional sales records |

---

# 🔍 Exploration Workflow

The exploration process is divided into multiple analytical stages.

Each stage focuses on a different aspect of the business data.

---

# 1️⃣ Database Exploration

## 📌 Objective
Understand the technical structure of the warehouse.

## 🔎 Focus Areas
- Schemas
- Tables
- Columns
- Data types
- Metadata
- Table organization

## 📖 Why It Matters
Before analyzing data, it is important to understand:
- what datasets exist,
- how data is organized,
- and how tables relate to each other.

Database exploration helps validate:
- schema design,
- dimensional modeling,
- and warehouse structure.

## ❓ Example Questions
- What tables are available?
- What columns exist in each table?
- Which data types are used?
- How is the warehouse organized?

---

# 2️⃣ Dimension Exploration

## 📌 Objective
Analyze descriptive and categorical business entities.

## 🔎 Focus Areas
- Countries
- Product categories
- Product subcategories
- Customer attributes
- Product hierarchies

## 📖 Why It Matters
Dimension tables describe business entities.

These attributes are used to:
- group data,
- segment business activity,
- filter dashboards,
- and organize reporting structures.

Dimension exploration helps analysts understand:
- how business data is categorized,
- and how reporting dimensions are structured.

## ❓ Example Questions
- Which countries do customers belong to?
- What product categories exist?
- How are products grouped?
- What customer segments are available?

---

# 3️⃣ Date Exploration

## 📌 Objective
Analyze the temporal boundaries and historical coverage of the dataset.

## 🔎 Focus Areas
- Earliest transaction dates
- Latest transaction dates
- Historical coverage
- Customer age analysis
- Timeline validation

## 📖 Why It Matters
Businesses operate over time.

Understanding date ranges helps:
- validate data completeness,
- determine historical depth,
- support trend analysis,
- and evaluate data freshness.

## ❓ Example Questions
- What is the first recorded sale?
- What is the latest transaction?
- How many years of data exist?
- What is the customer age distribution?

---

# 4️⃣ Measures Exploration

## 📌 Objective
Calculate high-level business metrics and KPIs.

## 🔎 Focus Areas
- Revenue
- Quantity sold
- Orders
- Customers
- Product counts
- Average pricing

## 📖 Why It Matters
Measures represent quantitative business performance.

This stage creates foundational metrics used in:
- executive dashboards,
- business reporting,
- and KPI monitoring systems.

## 📊 Types of Aggregation

### High-Level Aggregation
Summarized business metrics.

Examples:
- Total Sales
- Total Customers
- Total Orders

### Detailed-Level Aggregation
Granular business analysis.

Examples:
- Revenue per customer
- Sales per product
- Quantity per order

## ❓ Example Questions
- What is the total sales revenue?
- How many products were sold?
- What is the average selling price?
- How many active customers exist?

---

# 5️⃣ Magnitude Analysis

## 📌 Objective
Compare measures across business dimensions.

## 🧠 Core Concept
Measure (Aggregate) by Dimension

## 📖 Why It Matters
Magnitude analysis helps determine:
- which business segments are most important,
- which categories dominate revenue,
- and where business activity is concentrated.

This analysis helps prioritize:
- products,
- customers,
- categories,
- and geographic regions.

## 📊 Examples
- Total sales by country
- Revenue by category
- Quantity sold by product
- Orders by customer

## ❓ Example Questions
- Which country generates the highest revenue?
- Which category sells the most products?
- Which customers contribute most to sales?

---

# 6️⃣ Ranking Analysis

## 📌 Objective
Rank business entities based on performance metrics.

## 📖 Why It Matters
Ranking analysis helps identify:
- top performers,
- low performers,
- growth opportunities,
- and operational weaknesses.

This type of analysis is widely used in:
- sales reporting,
- customer analysis,
- and executive dashboards.

---

## 🏆 Top-N Analysis

Used to identify highest-performing entities.

### Examples
- Top 5 products by revenue
- Top 10 customers by sales
- Top categories by quantity sold

### Business Value
Helps businesses:
- identify best-selling products,
- recognize valuable customers,
- and optimize marketing strategies.

---

## 📉 Bottom-N Analysis

Used to identify weakest-performing entities.

### Examples
- Lowest-selling products
- Customers with fewest orders
- Low-performing categories

### Business Value
Helps businesses:
- identify underperforming areas,
- improve operational strategies,
- and reduce inefficiencies.

---

# ⚙️ SQL Concepts Used

The exploration scripts use several important SQL analytical techniques.

---

## 📌 Aggregation Functions

Used to summarize numerical data.

### Functions
- `SUM()`
- `COUNT()`
- `AVG()`
- `MIN()`
- `MAX()`

---

## 📌 Analytical / Window Functions

Used for ranking and advanced calculations.

### Functions
- `RANK()`
- `DENSE_RANK()`
- `ROW_NUMBER()`

---

## 📌 Filtering & Grouping

Used for segmentation and organization.

### Clauses
- `DISTINCT`
- `GROUP BY`
- `ORDER BY`
- `WHERE`

---

## 📌 Joins

Used to combine multiple datasets.

### Join Types
- `LEFT JOIN`
- `INNER JOIN`

---

## 📌 Advanced SQL Techniques

Used for complex analytical logic.

### Techniques
- Subqueries
- Window Functions
- Derived Tables

---

# 🚀 Final Goal of This Layer

The exploration layer serves as the analytical foundation of the project.

Its purpose is to:
- validate business-ready datasets,
- understand analytical behavior,
- generate insights,
- and support downstream reporting workflows.

This layer prepares the warehouse for:
- dashboards,
- KPI reporting,
- business intelligence,
- and advanced analytics.
