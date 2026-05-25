# 📊 Advanced Data Analytics Layer

This folder contains SQL scripts used for advanced analytical exploration on the Gold Layer of the Data Warehouse.

This layer focuses on answering real business questions using advanced SQL techniques such as window functions, CTEs, subqueries, and complex aggregations.

It bridges the gap between raw analytical tables and business decision-making by converting data into meaningful insights.

---

# 🎯 Objectives

- Analyze business performance using SQL
- Identify trends and patterns in data
- Compare current vs historical performance
- Segment customers and products
- Measure contribution of business entities
- Support data-driven decision making
- Build KPI and reporting logic

---

# 🧠 Core Analytical Flow

All analysis follows a structured flow:

Business Question → SQL Logic → Insight → Decision

---

# ⚙️ Techniques Used

This layer uses:

- Complex SQL Queries
- Window Functions
- CTE (Common Table Expressions)
- Subqueries
- Aggregations
- Case-based logic
- Analytical reporting techniques

---

# 📂 Folder Structure

📦 advanced_data_analytics/
│
├── 01_change_over_time_analysis.sql
├── 02_cumulative_analysis.sql
├── 03_performance_analysis.sql
├── 04_data_segmentation.sql
├── 05_part_to_whole_analysis.sql
└── README.md

---

# 🗂️ Tables Used

- gold.fact_sales
- gold.dim_customers
- gold.dim_products

---

# 📊 Change Over Time Analysis (Trends)

Understand how business metrics change over time.

Core idea:
∑Measure BY Date Dimension → Trend

Used for:
- Sales trend analysis
- Seasonality detection
- Growth tracking

Business value:
Forecasting, strategic planning, trend insights

---

# 📈 Cumulative Analysis

Measures how values accumulate over time.

Core idea:
Running Total = Current + Previous Values

Used for:
- Revenue tracking
- Growth measurement
- Performance accumulation

Business value:
Financial reporting, growth monitoring

---

# 📉 Performance Analysis

Compares current performance against benchmarks.

Core idea:
Current Value − Reference Value

Examples:
- Current sales vs previous year sales
- Product vs average performance

Used for:
- KPI tracking
- Year-over-year analysis
- Performance evaluation

---

# 🧩 Data Segmentation

Groups data into meaningful business categories.

Core idea:
IF condition → Segment assignment

Examples:
- VIP / Regular / New customers
- High / Medium / Low value products

Used for:
- Customer targeting
- Business classification
- Marketing strategies

---

# 📊 Part-to-Whole Analysis

Shows contribution of each part to total performance.

Core idea:
(Part ÷ Whole) × 100

Used for:
- Revenue contribution
- Market share analysis
- Category performance comparison

---

# ⚙️ SQL Concepts Applied

Aggregation:
SUM(), AVG(), COUNT(), MIN(), MAX()

Window Functions:
SUM() OVER(), AVG() OVER(), LAG(), LEAD()

Logic Handling:
CASE statements

Structuring:
CTEs, subqueries, joins

---

# 🚀 Outcome

This layer helps convert raw data into actionable business insights.

It enables understanding of:
- What is happening
- Why it is happening
- How it is changing
- Where improvement is needed

---

This is the foundation of business intelligence and data-driven decision making.