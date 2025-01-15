![example workflow](https://github.com/zablon-oigo/nba-data-lake/actions/workflows/deploy.yml/badge.svg)


## NBA Data Lake
This project automates the creation of a data lake for NBA API using AWS services. Amazon S3 bucket stores both raw and processed data. Sample NBA data in JSON format is uploaded to the S3 bucket for analysis. AWS Glue database defines an external table, enabling seamless querying of the data through Amazon Athena.
This setup provides an efficient, fast, and cost-effective solution for performing analytics on the NBA dataset stored in the S3 bucket.
### Run Locally

Clone the project

```bash
  git clone https://github.com/zablon-oigo/nba-data-lake.git
```

Go to the project directory

```bash
  cd nba-data-lake
```

Install dependencies

```bash
  pip install -r requirements.txt
```

Deploy resources

```bash
  python main.py
```

#### Output
![image](https://github.com/user-attachments/assets/9c3a4123-0d60-45d1-908f-bc47b164e67b)
