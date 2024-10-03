from google.cloud import bigquery
import random

client = bigquery.Client()

def generate_fake_sales_data():
    products = ["Produto A", "Produto B", "Produto C", "Produto D", "Produto E"]
    categories = ["Eletrônicos", "Roupas", "Alimentos", "Casa", "Beleza"]
    suppliers = ["Fornecedor 1", "Fornecedor 2", "Fornecedor 3"]
    regions = ["Norte", "Sul", "Leste", "Oeste"]

    rows_to_insert = []
    
    for product in products:
        for month in range(1, 13): 
            year = 2024  
            quantity_sold = random.randint(1, 100)  
            sales_revenue = round(quantity_sold * random.uniform(10.0, 100.0), 2)  
            discount = round(random.uniform(0, 0.3), 2)  
            net_revenue = round(sales_revenue * (1 - discount), 2)  
            month_name = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", 
                          "Junho", "Julho", "Agosto", "Setembro", "Outubro", 
                          "Novembro", "Dezembro"][month - 1]  
            customer_rating = round(random.uniform(1.0, 5.0), 1) 
            units_in_stock = random.randint(0, 500)  

            rows_to_insert.append({
                "product_name": product,
                "quantity_sold": quantity_sold,
                "sales_revenue": sales_revenue,
                "month": month_name,
                "year": year,
                "category": random.choice(categories),
                "supplier": random.choice(suppliers),
                "discount": discount,
                "net_revenue": net_revenue,
                "region": random.choice(regions),
                "customer_rating": customer_rating,
                "units_in_stock": units_in_stock
            })

    return rows_to_insert


def insert_sales_data_into_bigquery(rows):
    table_id = "repsthays.repositorios.sales" 

    errors = client.insert_rows_json(table_id, rows)  
    if errors == []:
        print("Dados inseridos com sucesso.")
    else:
        print(f"Erros ao inserir dados: {errors}")


fake_sales_data = generate_fake_sales_data()
insert_sales_data_into_bigquery(fake_sales_data)