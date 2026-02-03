# Judul Project
```
ANALISIS PERFORMA PENJUALAN PRODUCT RETAIL X SEBAGAI INDIKATOR KELAYAKAN INVESTASI 
```
## Repository Outline
```
1. ddl.txt
    - QUerry untuk create table dan insert value ke postgreSQL
2. great_expectation.ipynb
    - great expectation yang berisi validasi data setelah proses ETL
3. DAG.py
    - berisi syntax DAG untuk melakukan extraksi data dari data source (postgresSQL) dilanjut dengan transformasi data dan load data ke warehouse data base (elasticsearch)
4. data_raw.txt
    - MErupakan data mentah dari ekstraksi postgreSQL
5. daat_clean.txt
    - Merupakan data hasil transformasi yang sudah siap dimasukkan ke server warehouse database
6. Image
    - berisi kumpulan visualisasi untuk analisis
```

## Problem Background
```
Seorang investor ditawarkan untuk menanamkan modal di sebuah perusahaan retail. Sebelum benar-benar bergabung, investor tersebut datang untuk berkonsultasi mengenai kelayakan perusahaan retail ini untuk diakuisisi. Untuk perusahaan ini sendiri memiliki laporan yang sangat lengkap dimulai terdiri dari  17 atribut (kolom) dengan rentan terhitung dari januari 2011 sampai desember 2014.
```

## Project Output
``` 
Menghasilkan dashboard visualisasi yang menggambarkan performa perusahaan sebagai pertimbangan apakah akan bergabung untuk berinvestasi atau tidak.
```

## Data
```
Dataset berisi data penjualan retail x yang terdiri dari 17 kolom didapatkan dari kaggle dengan
    URL = https://www.kaggle.com/datasets/braniac2000/retail-dataset
dengan rincian sebagai berikut:
    Order ID        : ID untuk setiap transaksi dilakukan 
    Order Date      : Waktu dilakukannya transaksi
    Customer Name   : Nama Pembeli
    Country         : Negara Pembeli
    State           : Negara Bagian Pembeli
    City            : Kota Asal Pembeli
    Region          : Wilayah Pembeli
    Segment         : Segment Product 
    Ship Mode       : Mode Pengiriman Barang 
    Category        : Category Product
    Sub-Category    : Sub-Category Setiap Prodcut Dari Category 
    Product Name    : Nama Product
    Discount        : Potongan Harga
    Sales           : Total Nilai Product Terjual
    Profit          : Keuntungan
    Quantity        : Total Product Terjual
    Feedback?       : Feedback Dari Consumer
```

## Method
```
Sebelum dilakukan analisis dilakukan
- extraksi data dari postgree sebagai source  
- transformasi guna membersihkan dari noise dan menyesuaikan tipe data 
- load data ke elasticsearch sebagai data warehouse
- validasi data dengan great expectation untuk QC
- visualisasi data dengan kibana sesuai kebutuhan analisis
```

## Stacks
```
bahasa pemrogaman
    - python
    - SQL
library
    - pandas
    - psycopg2
    - datetime
    - airflow (DAG)
    - airflow.operators.python (PythonOperator)
    - elastchsearch (Elasticsearch, helpers)
    - csv
    - re
container
    - docker
database
    - postgresSQL
    - elasticsearch
orchestractor
    - airflow
visualization
    - kibana
```

## Reference
```
URL dataset = https://www.kaggle.com/datasets/braniac2000/retail-dataset 
```


