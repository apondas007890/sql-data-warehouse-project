# 📊 Advanced Data Analytics Layer

The **Advanced Data Analytics Layer** is the analytical part of the Data Warehouse where raw business data is transformed into meaningful insights using advanced SQL techniques.

This layer focuses on answering real business questions by analyzing trends, performance, customer behavior, product contribution, and business growth patterns.

It sits on top of the **Gold Layer** and converts clean analytical tables into decision-making insights.

---

# 🧠 What is Advanced Data Analytics?

Advanced Data Analytics is the process of exploring data deeply to discover:

- Trends
- Patterns
- Performance changes
- Business opportunities
- Customer behavior
- Growth or decline indicators

Unlike basic reporting, advanced analytics does not only show numbers.

It explains:

- What happened
- Why it happened
- How it changed over time
- Which entities contributed the most
- What actions should be taken

---

# 🎯 Why Do We Use Advanced Analytics?

Businesses generate huge amounts of data every day.

Without analytics, the data has little value.

Advanced analytics helps organizations:

- Make data-driven decisions
- Track business growth
- Detect performance problems
- Identify top customers and products
- Compare current vs historical performance
- Understand market behavior
- Improve operational efficiency

---

# ⏰ When Do We Use Advanced Analytics?

Advanced analytics is used when businesses want to:

| Business Need | Example |
|---|---|
| Analyze trends | Sales growth over years |
| Compare performance | Current year vs previous year |
| Track KPIs | Revenue, profit, orders |
| Detect seasonality | Monthly sales patterns |
| Segment customers | VIP vs regular customers |
| Measure contribution | Category contribution to revenue |
| Monitor growth | Running revenue growth |
| Support executives | Strategic dashboards and reporting |

---

# 🏗️ Position in Data Warehouse Architecture

```text
Raw Data Sources
        ↓
Bronze Layer (Raw Data)
        ↓
Silver Layer (Cleaned & Transformed Data)
        ↓
Gold Layer (Business-Ready Data)
        ↓
Advanced Data Analytics Layer
        ↓
Business Insights & Decision Making
```

---

# 🎯 Objectives of This Layer

The main objectives are:

- Analyze business performance
- Generate business insights
- Support reporting and dashboards
- Build KPI logic
- Compare historical and current data
- Detect trends and seasonality
- Segment business entities
- Support strategic decisions

---

# ⚙️ Core SQL Techniques Used

This layer heavily relies on advanced SQL concepts.

---

## 🔹 Complex Queries

Used to solve real business problems involving:

- Multiple tables
- Multiple conditions
- Business calculations
- Nested logic

---

## 🔹 Window Functions

Window functions perform analytical calculations without collapsing rows.

### Common Window Functions

```sql
SUM() OVER()
AVG() OVER()
LAG()
LEAD()
ROW_NUMBER()
RANK()
DENSE_RANK()
```

### Used For

- Running totals
- Moving averages
- Ranking
- YOY analysis
- Trend analysis
- Performance comparison

---

## 🔹 CTE (Common Table Expressions)

CTEs help organize large queries into readable logical blocks.

### Example

```sql
WITH sales_cte AS (
    SELECT *
    FROM gold.fact_sales
)
SELECT *
FROM sales_cte;
```

### Benefits

- Cleaner code
- Easier debugging
- Better readability
- Reusable logic

---

## 🔹 Subqueries

Queries written inside another query.

### Used For

- Filtering
- Dynamic calculations
- Comparison logic
- Conditional analysis

---

## 🔹 Aggregation Functions

Used to summarize business metrics.

### Common Aggregations

```sql
SUM()
AVG()
COUNT()
MIN()
MAX()
```

### Used For

- Revenue analysis
- Customer count
- Sales summaries
- KPI calculations

---

## 🔹 CASE Statements

Used for conditional business logic and segmentation.

### Example

```sql
CASE
    WHEN sales > 10000 THEN 'High'
    WHEN sales > 5000 THEN 'Medium'
    ELSE 'Low'
END
```

### Used For

- Customer segmentation
- Product classification
- KPI categorization
- Business grouping

---

# 📂 Repository Structure

```text
📦 scripts/
│
└── 📁 advanced_data_analytics/
    │
    ├── 📄 README.md
    │
    ├── 📄 01_change_over_time_analysis.sql
    │
    ├── 📄 02_cumulative_analysis.sql
    │
    ├── 📄 03_performance_analysis.sql
    │
    ├── 📄 04_data_segmentation.sql
    │
    ├── 📄 05_part_to_whole_analysis.sql
```

---

# 🗂️ Gold Layer Tables Used

The analytical queries mainly use Gold Layer dimensional modeling tables.

---

## 📄 `gold.fact_sales`

The central transactional fact table.

### Contains

- Sales amount
- Quantity
- Orders
- Revenue
- Product references
- Customer references
- Time references

### Used For

- Trend analysis
- KPI calculations
- Revenue reporting
- Performance analysis

---

## 📄 `gold.dim_customers`

Customer dimension table.

### Contains

- Customer details
- Customer categories
- Demographics
- Customer attributes

### Used For

- Customer segmentation
- Customer behavior analysis
- Revenue contribution analysis

---

## 📄 `gold.dim_products`

Product dimension table.

### Contains

- Product details
- Product categories
- Brand information
- Product attributes

### Used For

- Product performance analysis
- Category contribution analysis
- Product segmentation

---

# 📊 Core Advanced Analytics Topics

---

# 1️⃣ Change Over Time Analysis (Trend Analysis)

Analyze how a business metric changes over time.

This analysis helps identify:

- Business growth
- Performance decline
- Seasonality
- Long-term trends

---

## 🧠 Core Concept

```text
∑ [Measure] BY [Date Dimension]
```

---

## 📌 Examples

```text
Total Sales BY Year
Average Sales BY Month
Total Orders BY Quarter
```

---

## 📈 Year-Level Analysis

Provides high-level strategic insights.

### Business Value

- Growth tracking
- Forecasting
- Strategic planning
- Executive reporting

---

## 📅 Month-Level Analysis

Provides detailed operational insights.

### Business Value

- Detect seasonality
- Understand monthly fluctuations
- Analyze demand patterns

---

## ⚙️ Common SQL Used

```sql
GROUP BY
DATE FUNCTIONS
AGGREGATIONS
WINDOW FUNCTIONS
```

---

# 2️⃣ Cumulative Analysis

Analyze how values accumulate progressively over time.

This helps determine whether the business is continuously growing or declining.

---

## 🧠 Core Concept

```text
Running Total = Current Value + Previous Values
```

---

## 📌 Examples

### Running Total

```text
Running Total Sales BY Month
```

### Moving Average

```text
Moving Average Sales BY Month
```

---

## 🔍 Business Value

Used for:

- Revenue growth monitoring
- Financial reporting
- Business momentum tracking
- Long-term growth analysis

---

## ⚙️ Main SQL Technique

```sql
SUM() OVER()
AVG() OVER()
```

### Example

```sql
SUM(sales_amount) OVER(
    ORDER BY order_date
)
```

---

# 3️⃣ Performance Analysis

Compare current performance against reference values or benchmarks.

This helps measure business success and efficiency.

---

## 🧠 Core Concept

```text
Current Value − Reference Value
```

---

## 📌 Common Examples

### Current vs Average

```text
Current Sales − Average Sales
```

### Current Year vs Previous Year (YOY)

```text
Current Year Sales − Previous Year Sales
```

### Current vs Best/Worst

```text
Current Sales − Lowest Sales
```

---

## 🔍 Business Value

Used for:

- KPI tracking
- Benchmark analysis
- Performance evaluation
- YOY analysis

---

## ⚙️ Main SQL Technique

```sql
LAG()
LEAD()
AVG() OVER()
```

---

# 4️⃣ Data Segmentation

Group data into meaningful business categories or ranges.

Segmentation helps businesses understand relationships between metrics and classify entities into groups.

---

## 🧠 Core Concept

```text
IF Condition → Segment Assignment
```

---

## 📌 Common Examples

### Customer Segmentation

```text
VIP / Regular / New Customers
```

### Product Segmentation

```text
High / Medium / Low Value Products
```

### Sales Range Segmentation

```text
Products BY Sales Range
```

### Age Group Segmentation

```text
Customers BY Age Group
```

---

## 🔍 Business Value

Used for:

- Customer targeting
- Marketing campaigns
- Product categorization
- Behavioral analysis

---

## ⚙️ Main SQL Technique

```sql
CASE Statements
```

### Example

```sql
CASE
    WHEN sales > 5000 THEN 'High'
    WHEN sales > 2000 THEN 'Medium'
    ELSE 'Low'
END
```

---

# 5️⃣ Part-to-Whole Analysis (Proportional Analysis)

Analyze how individual entities contribute to the overall business performance.

This identifies which products, categories, customers, or regions contribute the most.

---

## 🧠 Core Concept

```text
([Part] / Total [Measure]) × 100
```

---

## 📌 Common Examples

### Revenue Contribution

```text
(Sales / Total Sales) × 100 BY Category
```

### Quantity Contribution

```text
(Quantity / Total Quantity) × 100 BY Country
```

---

## 🔍 Business Value

Used for:

- Revenue contribution analysis
- Market share analysis
- Category comparison
- Identifying top contributors

---

## ⚙️ Common SQL Used

```sql
SUM()
WINDOW FUNCTIONS
PERCENTAGE CALCULATIONS
```

---

# 🔄 End-to-End Analytical Workflow

```text
Business Data
        ↓
Data Warehouse
        ↓
Gold Layer Tables
        ↓
Advanced SQL Analytics
        ↓
Business Insights
        ↓
Reports & Dashboards
        ↓
Strategic Decisions
```

---

# 📊 What This Layer Ultimately Provides

This layer converts raw warehouse data into:

- Business intelligence
- Strategic insights
- KPI measurements
- Growth indicators
- Executive reporting
- Analytical dashboards

---

# 🚀 Final Outcome

The Advanced Data Analytics Layer helps organizations:

✅ Understand business performance  
✅ Monitor growth and decline  
✅ Compare historical trends  
✅ Detect seasonality  
✅ Segment customers and products  
✅ Measure business contribution  
✅ Support strategic planning  
✅ Make data-driven decisions  

---

# 🏁 Conclusion

The Advanced Data Analytics Layer is one of the most important layers in modern Business Intelligence systems.

It transforms structured warehouse data into actionable business insights using advanced SQL analytics.

```text
Raw Data
    ↓
Analytics
    ↓
Insights
    ↓
Decisions
    ↓
Business Growth
```

This layer enables businesses to move beyond simple reporting and truly understand their data.
