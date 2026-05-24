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

The exploration scripts analyze the Gold Layer of the Data Warehouse.

The Gold Layer contains:
- cleaned,
- transformed,
- integrated,
- and analytics-ready datasets.

These tables are optimized for:
- reporting,
- KPI analysis,
- dashboarding,
- and business intelligence.

---

# 📂 Exploration Folder Structure

```text
📦 scripts/
│
└── 📁 exploration/
    │
    │
    ├── 📄 README.md
    │
    ├── 📄 01_database_exploration.sql     # Explore schemas, tables, columns, and metadata
    │
    ├── 📄 02_dimension_exploration.sql    # Analyze dimensions and categorical attributes
    │
    ├── 📄 03_date_range_exploration.sql   # Analyze historical timelines and date coverage
    │
    ├── 📄 04_measures_exploration.sql     # Calculate key business metrics and KPIs
    │
    ├── 📄 05_magnitude_analysis.sql       # Compare measures across business dimensions
    │
    └── 📄 06_ranking_analysis.sql         # Rank entities based on business performance

```

---

# 🗂️ Gold Layer Tables Used

| Table Name | Description |
|---|---|
| `gold.dim_customers` | Customer dimension table |
| `gold.dim_products` | Product dimension table |
| `gold.fact_sales` | Sales transactional fact table |

---

# 🔍 Exploration Workflow

The exploration process is divided into multiple analytical stages.

Each stage focuses on a different perspective of the business data.

---

# 1️⃣ Database Exploration

## 🎯 Objective
Understand the technical structure of the warehouse.

---

## 🔎 Focus Areas
- Schemas
- Tables
- Columns
- Data types
- Metadata

---

## 🧠 Core Concept

```text
Database Structure → Tables → Columns → Relationships
```

---

## 📖 Purpose

Database exploration helps:
- understand warehouse structure,
- inspect schemas and metadata,
- identify available datasets,
- and validate table organization.

---

## ❓ Example Questions
- What tables exist?
- What columns are available?
- Which schemas contain business data?
- What data types are used?

---

# 2️⃣ Dimension Exploration

## 🎯 Objective
Analyze descriptive and categorical business entities.

---

## 🔎 Focus Areas
- Countries
- Categories
- Subcategories
- Customer attributes
- Product hierarchies

---

## 🧠 Core Concept

```text
Identify Unique Values (Categories) in Each Dimension
```

```text
Dimension → Grouping → Segmentation → Analysis
```

---

## 📖 Purpose

Dimension exploration helps recognize:
- how business data can be grouped,
- how entities are segmented,
- and how reporting dimensions are structured.

This is useful for:
- filtering,
- dashboard slicing,
- segmentation,
- and business categorization.

---

## ❓ Example Questions
- Which countries do customers belong to?
- What product categories exist?
- How are products organized?

---

# 3️⃣ Date Exploration

## 🎯 Objective
Analyze the temporal boundaries of the dataset.

---

## 🔎 Focus Areas
- Earliest dates
- Latest dates
- Historical coverage
- Timeline validation
- Customer age analysis

---

## 🧠 Core Concept

```text
MIN(Date) → Earliest Record
MAX(Date) → Latest Record
```

```text
Time Boundaries → Historical Scope → Trend Analysis
```

---

## 📖 Purpose

Date exploration helps:
- determine historical depth,
- validate data freshness,
- understand timespan,
- and prepare trend analysis.

---

## ❓ Example Questions
- What is the first order date?
- What is the latest transaction date?
- How many years of data exist?

---

# 4️⃣ Measures Exploration

## 🎯 Objective
Calculate key business metrics and KPIs.

---

## 🔎 Focus Areas
- Revenue
- Quantity
- Orders
- Customers
- Products
- Pricing

---

## 🧠 Core Concept

```text
Highest Level of Aggregation
```

```text
∑Sales | AVG(Price) | ∑Quantity
```

```text
Measures = Big Business Numbers
```

---

## 📖 Purpose

Measures represent quantitative business performance.

This stage creates foundational KPIs used in:
- executive reporting,
- dashboards,
- business monitoring,
- and performance tracking.

---

## ❓ Example Questions
- What is total revenue?
- How many products were sold?
- What is the average selling price?
- How many active customers exist?

---

# 5️⃣ Magnitude Analysis

## 🎯 Objective
Compare business measures across dimensions.

---

## 🧠 Core Concept

```text
∑Measure (Aggregate) By Dimension
```

### Examples

```text
∑Sales By Country
∑Quantity By Category
AVG(Price) By Product
∑Orders By Customer
```

---

## 📖 Purpose

Magnitude analysis helps understand:
- the importance of business categories,
- dominant revenue contributors,
- customer contribution levels,
- and sales distribution patterns.

It answers:
- Which category performs best?
- Which country generates the most revenue?
- Which customers contribute most to sales?

---

## ❓ Example Questions
- Which category generates the highest revenue?
- Which country sells the most products?
- Which customers generate the most sales?

---

# 6️⃣ Ranking Analysis

## 🎯 Objective
Order business entities based on performance measures.

---

## 🧠 Core Concept

```text
Rank[Dimension] By ∑Measure (Aggregate)
```

### Examples

```text
Rank Countries By ∑Sales
Rank Products By ∑Quantity
Rank Customers By ∑Revenue
```

---

## 📊 Ranking Types

### 🏆 Top-N Performers

Identify highest-performing entities.

Examples:
- Top 5 products by revenue
- Top 10 customers by sales
- Top categories by quantity sold

---

### 📉 Bottom-N Performers

Identify lowest-performing entities.

Examples:
- Bottom 5 products by revenue
- Customers with fewest orders
- Low-performing categories

---

## 📖 Purpose

Ranking analysis helps businesses:
- identify top performers,
- detect weak-performing areas,
- optimize strategies,
- and prioritize business decisions.

---

# ⚙️ SQL Concepts Used

The exploration scripts use several important SQL analytical techniques.

---

## 📌 Aggregation Functions

Used to summarize numerical values.

```sql
SUM()
COUNT()
AVG()
MIN()
MAX()
```

---

## 📌 Analytical / Window Functions

Used for ranking and advanced analysis.

```sql
RANK()
DENSE_RANK()
ROW_NUMBER()
```

---

## 📌 Filtering & Grouping

Used for segmentation and organization.

```sql
DISTINCT
GROUP BY
ORDER BY
WHERE
```

---

## 📌 Joins

Used to combine multiple datasets.

```sql
LEFT JOIN
INNER JOIN
```

---

## 📌 Advanced SQL Techniques

Used for complex analytical logic.

```sql
Subqueries
Window Functions
Derived Tables
```

---

# 🚀 Final Goal of This Exploration Layer

The exploration layer acts as the analytical foundation of the project.

Its purpose is to:
- validate analytical datasets,
- understand business behavior,
- generate meaningful insights,
- and support downstream BI workflows.

This layer prepares the warehouse for:
- dashboard development,
- KPI reporting,
- business intelligence,
- and advanced analytics.
