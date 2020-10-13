## 00_Amazon_data

Scripts to process Amazon data.
- `00_upload_reviews.py`: Upload Amazon product reviews to AWS S3 bucket.
- `01_count_reviews.py`: Use Spark to count number of reviews per product.
- `02_select_meta.py`: Filter metadata based on number of reviews and product category. Keep only useful fields in metadata.
- `03_select_reviews.py`: Keep only reviews that match filtered metadata.
