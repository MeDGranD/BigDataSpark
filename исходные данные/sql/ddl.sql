DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_pet_type CASCADE;
DROP TABLE IF EXISTS dim_pet_breed CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS dim_size CASCADE;
DROP TABLE IF EXISTS dim_color CASCADE;
DROP TABLE IF EXISTS dim_material CASCADE;
DROP TABLE IF EXISTS dim_brand CASCADE;
DROP TABLE IF EXISTS dim_pet_category CASCADE;
DROP TABLE IF EXISTS dim_pet CASCADE;
DROP TABLE IF EXISTS dim_state CASCADE;
DROP TABLE IF EXISTS dim_city CASCADE;
DROP TABLE IF EXISTS dim_product_category CASCADE;


-- Справочники для снежинки

CREATE TABLE dim_product_category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    pet_category_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_brand (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_material (
    material_id SERIAL PRIMARY KEY,
    material_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_color (
    color_id SERIAL PRIMARY KEY,
    color_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_size (
    size_id SERIAL PRIMARY KEY,
    size_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_state (
    state_id SERIAL PRIMARY KEY,
    state_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_pet_breed (
    breed_id SERIAL PRIMARY KEY,
    breed_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE dim_pet_type (
    pet_type_id SERIAL PRIMARY KEY,
    pet_type_name VARCHAR(255) UNIQUE NOT NULL
);

-- Измерения звезды

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    weekday INT NOT NULL, --- (DOW)
    week_of_year INT NOT NULL,
    day_name VARCHAR(255) NOT NULL,
    month_name VARCHAR(255) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255),
    supplier_contact_person VARCHAR(255),
    supplier_email VARCHAR(255),
    supplier_phone VARCHAR(255),
    supplier_address VARCHAR(255),
    city_id INT,
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id)
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255),
    store_location VARCHAR(255),
    city_id INT,
    state_id INT,
    country_id INT,
    store_phone VARCHAR(255),
    store_email VARCHAR(255),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (state_id) REFERENCES dim_state(state_id)
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category_id INT,
    brand_id INT,
    material_id INT,
    color_id INT,
    size_id INT,
    product_quantity INT,
    product_current_price DECIMAL(10, 2),
    product_weight_gr DECIMAL(6, 1),
    product_description TEXT,
    product_avg_rating DECIMAL(2, 1),
    product_reviews_count INT,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_id INT,
    FOREIGN KEY (category_id) REFERENCES dim_product_category(category_id),
    FOREIGN KEY (brand_id) REFERENCES dim_brand(brand_id),
    FOREIGN KEY (material_id) REFERENCES dim_material(material_id),
    FOREIGN KEY (color_id) REFERENCES dim_color(color_id),
    FOREIGN KEY (size_id) REFERENCES dim_size(size_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE dim_pet (
    pet_id SERIAL PRIMARY KEY,
    pet_type_id INT,
    pet_category_id INT,
    customer_pet_name VARCHAR(255),
    pet_breed_id INT,
    FOREIGN KEY (pet_type_id) REFERENCES dim_pet_type(pet_type_id),
    FOREIGN KEY (pet_breed_id) REFERENCES dim_pet_breed(breed_id),
    FOREIGN KEY (pet_category_id) REFERENCES dim_pet_category(pet_category_id)
);


CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(255),
    customer_last_name VARCHAR(255),
    customer_age INT,
    customer_email VARCHAR(255),
    country_id INT,
    customer_postal_code VARCHAR(255),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
);


CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    seller_first_name VARCHAR(255),
    seller_last_name VARCHAR(255),
    seller_email VARCHAR(255),
    country_id INT,
    seller_postal_code VARCHAR(255),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
);


-- Факты звезды

CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL,
    customer_id INT NOT NULL,
    seller_id INT NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    supplier_id INT NOT NULL,
    sale_quantity INT NOT NULL CHECK (sale_quantity > 0),
    sale_total_price DECIMAL(10, 2) NOT NULL,
    original_raw_id INT,
    pet_id INT,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (seller_id) REFERENCES dim_seller(seller_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
    FOREIGN KEY (pet_id) REFERENCES dim_pet(pet_id)
);

--- Индексы для быстрого поиска и анализа

CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_id);
CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_id);
CREATE INDEX idx_fact_sales_pet ON fact_sales(pet_id);