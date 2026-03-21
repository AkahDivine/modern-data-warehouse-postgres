
# Data Dictionary: Gold Layer

### Overview:
The Gold Layer represents the business-ready, analytics-optimized data model.
It is structured using a dimensional model (star schema), consisting of
dimension tables and fact tables.

- Dimension tables store descriptive attributes (who, what, where).
- Fact tables store measurable business events (how much, how many).

## This layer is designed for reporting, dashboards, and business intelligence use cases



### **1. gold.dim_customers**

**Purpose:** Stores enriched customer data with demographic and geographic attributes.

```markdown
| Column Name     | Data Type   | Description |
|-----------------|------------|-------------|
| customer_key    | INT        | Surrogate key uniquely identifying each customer record in the dimension table. |
| customer_id     | INT        | Unique numerical identifier assigned to each customer. |
| customer_number | VARCHAR(50)| Alphanumeric identifier used for tracking and referencing. |
| first_name      | VARCHAR(50)| Customer's first name. |
| last_name       | VARCHAR(50)| Customer's last name or family name. |
| country         | VARCHAR(50)| Country of residence (e.g., 'Germany'). |
| marital_status  | VARCHAR(50)| Marital status (e.g., 'Married', 'Single', 'n/a'). |
| gender          | VARCHAR(50)| Gender (e.g., 'Male', 'Female', 'n/a'). |
| birthdate       | DATE       | Date of birth (YYYY-MM-DD). |
| create_date     | TIMESTAMP  | Timestamp when the record was created. |
```

---

### **2. gold.dim_products**

**Purpose:** Stores product attributes and classification details.

```markdown
| Column Name          | Data Type   | Description |
|----------------------|------------|-------------|
| product_key          | INT        | Surrogate key uniquely identifying each product. |
| product_id           | INT        | Unique identifier assigned to the product. |
| product_number       | VARCHAR(50)| Alphanumeric product code for tracking. |
| product_name         | VARCHAR(50)| Descriptive product name. |
| category_id          | VARCHAR(50)| Identifier linking product to its category. |
| category             | VARCHAR(50)| High-level classification (e.g., 'Bikes'). |
| subcategory          | VARCHAR(50)| Detailed classification within the category. |
| maintenance_required | VARCHAR(50)| Indicates if maintenance is required ('Yes', 'No', 'n/a'). |
| cost                 | INT        | Base cost of the product. |
| product_line         | VARCHAR(50)| Product line (e.g., 'Road', 'Mountain'). |
| start_date           | DATE       | Date the product became available. |
```

---

### **3. gold.fact_sales**

**Purpose:** Stores transactional sales data for analytics and reporting.

```markdown
| Column Name   | Data Type   | Description |
|---------------|------------|-------------|
| order_number  | VARCHAR(50)| Unique identifier for each sales order (e.g., 'SO54496'). |
| product_key   | INT        | Foreign key referencing dim_products. |
| customer_key  | INT        | Foreign key referencing dim_customers. |
| order_date    | DATE       | Date when the order was placed. |
| shipping_date | DATE       | Date when the order was shipped. |
| due_date      | DATE       | Payment due date. |
| sales_amount  | INT        | Total sales value for the transaction. |
| quantity      | INT        | Number of units sold. |
| price         | INT        | Price per unit. |
```



## **Key Notes**

* All dimension tables use **surrogate keys** (`*_key`) for efficient joins.
* Fact table uses **foreign keys** to connect to dimension tables.
* Data types follow **PostgreSQL standards** (`VARCHAR`, `DATE`, `TIMESTAMP`).
* This structure supports **star schema design** for fast analytical queries.

