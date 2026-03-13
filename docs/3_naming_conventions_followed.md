# Naming Conventions

This document defines the **naming standards used in the Data Warehouse project**.  
Consistent naming conventions improve **readability, maintainability, and collaboration**.

---

## 1. General Rules

- The project uses the **snake_case** naming convention.
- Words are written in **lowercase and separated with underscores**.

Example:
	customer_info
	sales_details
	product_category

Additional guidelines:

- Avoid using **SQL reserved keywords** as table or column names.
- Use **clear and descriptive names** aligned with the business meaning.

Example to avoid:
	table
	order
	group

---

## 2. Table Naming Conventions

The data warehouse follows a **Medallion Architecture** with three layers: Bronze, Silver, and Gold.

### Bronze Layer

The **Bronze layer** stores raw data exactly as received from the source systems.

**Rules**

- Table names must start with the **source system name**.
- Table names must match the **original source table name**.
- No renaming should occur.

**Pattern**
		<sourcesystem>_<entity>
	
Example:
	crm_customer_info
	
Customer information table from the **CRM system**.

---

### Silver Layer

The **Silver layer** contains cleaned and standardized data.

**Rules**

- Table names must start with the **source system name**.
- Table names should remain consistent with the original source table names.

**Pattern**
	<sourcesystem>_<entity>
	
Example:
	crm_customer_info

Customer data after **cleaning and transformation**.

---

### Gold Layer

The **Gold layer** contains business-ready tables optimized for **analytics and reporting**.

**Rules**

- Table names must be **business-friendly and descriptive**.
- Table names must begin with a **category prefix**.

**Pattern**
	<category>_<entity>

Examples:
	dim_customers
	dim_products
	fact_sales

---

## 3. Category Prefix Glossary

| Prefix | Meaning | Example |
|------|------|------|
| `dim_` | Dimension table | dim_customers |
| `fact_` | Fact table | fact_sales |
| `agg_` | Aggregated table | agg_sales_monthly |

---

## 4. Column Naming Conventions

### Surrogate Keys

All **dimension table primary keys** must use the `_key` suffix.

Pattern:
	<table_name>_key

Example:
	customer_key

Surrogate key used in the **dim_customers** table.

---

### Technical Columns

Technical metadata columns must use the **`dwh_` prefix**.

Pattern:
	dwh_<column_name>

Example:
	dwh_load_date

This column stores the **timestamp when a record was loaded into the data warehouse**.

---

## 5. Stored Procedure Naming

Stored procedures responsible for loading data must follow this pattern:
	load_<layer>

Examples:
	load_bronze
	load_silver
	load_gold
	
| Procedure | Purpose |
|----------|--------|
| `load_bronze` | Loads raw data into the Bronze layer |
| `load_silver` | Applies data cleansing and transformation |
| `load_gold` | Creates analytical tables for reporting |

---

## Summary

These naming conventions ensure:

- Consistent **table and column naming**
- Clear **data warehouse layer identification**
- Better **code readability and maintainability**
- Standardized **data engineering practices**