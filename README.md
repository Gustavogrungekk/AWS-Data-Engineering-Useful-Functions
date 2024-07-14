# AWS Data Engineering Useful Functions

Welcome to the AWS Data Engineering Useful Functions repository! This repository is dedicated to housing a collection of essential functions that I've developed and curated to streamline various data engineering tasks within AWS environments.

## Purpose

As a Data Engineer, I frequently encounter repetitive tasks that demand efficient solutions. This repository aims to centralize and standardize these solutions in the form of reusable functions. Whether it's data processing, ETL operations, or data transformations, these functions are designed to enhance productivity and maintain consistency across projects.

## Repository Contents

### AWSesome_modules.py

The core of this repository is the `AWSesome_modules.py` file, which contains a variety of Python functions tailored for AWS data engineering tasks. These functions cover a wide range of functionalities, including:

- Data extraction from various sources (S3, databases, APIs)
- Data transformation and cleansing operations
- Integration with AWS Glue jobs for ETL processes
- Data loading into AWS data warehouses (Redshift, Athena, etc.)

### Other Files

- **README.md**: You are currently reading it! This file provides an overview of the repository's purpose, contents, and how to utilize the functions effectively.
- **License**: Details the terms under which the functions in this repository are distributed.

## Getting Started

To start using these functions effectively:

1. **Clone the Repository**: Clone this repository to your local environment or directly integrate it into your AWS environment.

2. **Explore AWSesome_modules.py**: Dive into `AWSesome_modules.py` to discover the range of functions available and their specific use cases.

3. **Integration with AWS Glue Jobs**: `AWSesome_modules.py` is designed for seamless integration with AWS Glue jobs. Here's how you can deploy it:

   ### Configuring Glue Job to Use Python Libraries from S3

   1. **Navigate to Glue Job Details**
      - Go to your AWS Management Console.
      - Open the AWS Glue console.

   2. **Select Your Glue Job**
      - Find and select the Glue job for which you want to configure Python libraries.

   3. **Access Advanced Properties**
      - In the job details page, navigate to the **Advanced properties** section.

   4. **Configure Python Libraries**
      - Locate the **Python library path** under the **Libraries** section.
      - Click on **Add library path**.

   5. **Specify S3 Path**
      - Enter the S3 path where your Python file (`AWSesome_modules.py`) is stored.
      - Example: `s3://your-bucket-name/path/to/AWSesome_modules.py`

   6. **Save Changes**
      - Save the changes to apply the new library configuration.

   7. **Import and Use in Glue Job**
      - In your Glue job script, import the functions from `AWSesome_modules.py` using Python's `import` statement.
      ```python
      from AWSesome_modules import your_function_name
      ```
      - Now you can use `your_function_name()` or any other function from `AWSesome_modules.py` within your Glue job script.

   ### Example Usage

   Assuming `AWSesome_modules.py`  `log()`:
   ```python
   from AWSesome_modules import log

   # Using the log function
   log("Logging information from AWS Glue job.")


4. **Contribute**: Feel free to contribute additional functions or improvements via pull requests. Your contributions help expand the utility of this repository for the broader AWS data engineering community.

## Feedback and Contributions

Your feedback and contributions are highly appreciated! If you have suggestions, improvements, or new functions to add, please submit a pull request or reach out via issues.