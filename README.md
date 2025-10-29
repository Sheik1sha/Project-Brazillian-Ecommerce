# 🛒 Brazilian E-Commerce Data Engineering Project  

## 📘 Project Overview  
This project demonstrates an **end-to-end Azure-based data engineering pipeline** built using the **Brazilian E-Commerce (Olist)** dataset.  
The goal is to design, automate, and visualize the entire data lifecycle — from ingestion to analytics-ready dashboards — using modern data engineering tools on Azure.

---

## 🧩 Architecture  
![Architecture](Architecture_Diagram/ArchitectureDiagram.png)

### 🔹 Data Flow Summary  
1. **Data Ingestion – Azure Data Factory**  
   - Pulls data from **GitHub (HTTP)** and **SQL Tables**.  
   - Stores data into **Azure Data Lake Storage Gen2 (Raw Zone)**.  

2. **Data Transformation – Azure Databricks**  
   - Reads raw data from ADLS.  
   - Cleans and joins tables using **PySpark** and **Spark SQL**.  
   - Performs enrichment by merging additional reference data from **MongoDB**.  
   - Writes curated data back to ADLS (Transformed Zone).  

3. **Data Modeling – Azure Synapse Analytics**  
   - Creates **external tables** on top of transformed Parquet files.  
   - Enables SQL-based analytics and BI connections.  

4. **Visualization & Insights**  
   - Connects **Power BI**, **Tableau**, and **Microsoft Fabric** to Synapse for dashboards.  
   - Dashboards highlight:
     - Sales and revenue trends  
     - Payment behavior  
     - Delivery delays and logistics performance  
     - Customer and seller insights  

---

## 🧱 Tech Stack  
| Layer | Tools & Services |
|-------|------------------|
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 |
| Transformation | Azure Databricks (PySpark, SQL) |
| Enrichment | MongoDB |
| Data Warehouse | Azure Synapse Analytics |
| Visualization | Power BI, Tableau, Microsoft Fabric |

---

## 🗂️ Dataset Overview  

**Source:** [Olist Brazilian E-Commerce Dataset (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)  

The dataset consists of multiple relational tables describing orders, customers, sellers, payments, reviews, and products from the Olist e-commerce platform.

---

## 🧮 Data Model / Schema  
Below is the schema representing how the datasets are related:

![Schema](architecture/olist_schema_diagram.png)

### Key Relationships  
| Table | Key Column | Linked Table | Relationship |
|--------|-------------|---------------|--------------|
| `olist_orders_dataset` | `order_id` | `olist_order_items_dataset` | 1 → Many |
| `olist_orders_dataset` | `order_id` | `olist_order_reviews_dataset` | 1 → Many |
| `olist_orders_dataset` | `order_id` | `olist_order_payments_dataset` | 1 → Many |
| `olist_orders_dataset` | `customer_id` | `olist_order_customer_dataset` | Many → 1 |
| `olist_order_items_dataset` | `product_id` | `olist_products_dataset` | Many → 1 |
| `olist_order_items_dataset` | `seller_id` | `olist_sellers_dataset` | Many → 1 |
| `olist_sellers_dataset` | `zip_code_prefix` | `olist_geolocation_dataset` | Many → 1 |
| `olist_order_customer_dataset` | `zip_code_prefix` | `olist_geolocation_dataset` | Many → 1 |

---

## ⚙️ Key Features
- Automated **ETL pipeline** using Azure Data Factory and Databricks.  
- Multi-layered data lake structure (**Raw → Transformed → Analytics**).  
- **Incremental refresh**, **schema validation**, and **error handling**.  
- Data enrichment using external MongoDB tables.  
- BI dashboards connected via Synapse external tables.  

---
## 📊 Dashboard Previews  

### 🟣 **Sales & Delivery Dashboard**
![Dashboard 1](Dashboard/Dashboard1.png)

**Highlights:**
- KPIs for total sales, unique customers, on-time delivery %, and average review score.  
- **Sales Overview** – Monthly revenue trend.  
- **Payment Analysis** – Breakdown of payment types (credit, boleto, debit).  
- **Top Categories by Revenue** – Identifies highest-performing product categories.  
- **Geographic Sales & Satisfaction** – Map view of customer ratings and sales density.

---

### 🟠 **Product Performance & Satisfaction Dashboard**
![Dashboard 2](Dashboard/dashboard2.png)

**Highlights:**
- **Top Product Categories** – Visual breakdown of category-level sales volume.  
- **Average Delivery Lead Time by State** – Map visualization of delivery performance across Brazil.  
- **Customer Review Distribution** – Sentiment insights based on review scores.  
- **Impact of Delivery Timeliness** – Analyzes how delivery time affects satisfaction ratings.
---

## 📁 Repository Structure

├── data/
│ ├── raw/
│ └── transformed/
├── notebooks/
│ ├── Data_ingestion.ipynb
│ ├── databricks_code_transformation.py
├── sql/
│ └── synapse_gold_layer_tables.sql
├── reports/
│ └── dashboards.pbix
├── dashboards/
│ ├── brazilian_ecommerce_dashboard.png
│ └── product_performance_dashboard.png
├── architecture/
│ ├── architecture_diagram.png
│ └── olist_schema_diagram.png
└── README.md
