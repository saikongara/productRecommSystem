# Recommendation model 
Calculates best suitable products of a eCommerce client for a Customer based on Transaction history

Datasets used to feed the model
- Products (*.txt*)
- Product Categories (*.tsv*)
- Transactions (*complex JSON*)

Technologies used
- Databricks (Spark 2.4.6)
- Python 3.6
- Google Cloud Platform (GCP)
- Big Query
- Rest API calls

## **Model breakdown**:

- The model feeds on products, categories and transaction datasets and identifies highly purchased products(and corresponding categories) based on transaction history.
- The transaction dataset doesn't have driving column; made use of Python uuid to generate a random unique column values to perform self join on transaction dataset to identify highly transacted products and categories.
- Retrieve test service engine credentials by making REST calls from GCP using service account details provided from input file.
- Prepared JSON request out of identified highly transacted dataset to perform REST call to TEST service engine hosted on GCP to validate the model performance.

## **Future Scope**:
- Self Learning - **scikit** library implementation.
- Integrate **Email notification** about recommended products to customers.