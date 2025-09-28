# archive-io


Got it 👍 here’s the **full `README.md` file**, ready for copy-paste into your repo.

---

```markdown
# Archive-IO (AWS Terraform Project)

Archive-IO is an AWS-based **data archival and validation workflow** deployed with **Terraform**.  
It automates the process of archiving data from a source RDBMS into **AWS Athena Iceberg**, validating source vs target data, generating reports, and storing metadata.  

The project uses **API Gateway, Lambda, Glue, DynamoDB, Step Functions, S3, and IAM**.

---

## 🚀 Features

- **API Gateway** → Entry point (`archival_flow` resource)
- **Step Functions** → Orchestrates the archival → validation → reporting flow
- **Glue Jobs**:
  - `archival` → moves data from RDBMS → Iceberg
  - `validation` → checks schema, row count, checksums
  - `purge` → optional purge functionality
- **Lambda Functions**:
  - `archival_flow_trigger` → triggers Step Functions
  - `report_generation` → generates PDF reports, uploads to S3
- **DynamoDB** → Stores archival and purge metadata
- **S3** → Stores archived data and PDF reports
- **IAM Roles** → For Lambda, Glue, and Step Functions
- **CloudWatch Logs** → Centralized logging for all components

---

## 📂 Project Structure

```

archive-io/
├── terraform_scripts/          # Terraform IaC for all AWS resources
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   ├── provider.tf
│   ├── terraform.tfvars
│   ├── deploy.sh               # Deployment script (dev/qa/prod)
│   └── env/                    # Environment-specific variables
│       ├── dev.tfvars
│       ├── qa.tfvars
│       └── prod.tfvars
│
├── lambda_functions/           # AWS Lambda source code
│   ├── archival_flow_trigger/
│   │   └── handler.py
│   └── report_generation/
│       └── handler.py
│
├── glue_jobs/                  # AWS Glue ETL job scripts
│   ├── archival/
│   │   └── archival_job.py
│   ├── validation/
│   │   └── validation_job.py
│   └── purge/
│       └── purge_job.py
│
├── input_payloads/             # Sample input payloads for API Gateway
│   └── archival_input_payload.json
│
└── logs/                       # CloudWatch / debugging logs
└── clpidwatch/

````

---

## ⚙️ Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads)
- [Docker Desktop](https://www.docker.com/products/docker-desktop) (must be **installed and running**)
- AWS CLI configured with proper credentials and access

---

## 🛠️ Deployment

1. Navigate to the `terraform_scripts` folder:

```bash
cd terraform_scripts
````

2. Run the deployment script for the target environment (`dev`, `qa`, `prod`):

```bash
./deploy.sh dev
```

3. Terraform will provision:

   * API Gateway
   * Lambda functions
   * Glue Jobs
   * Step Functions
   * DynamoDB tables
   * IAM roles
   * S3 buckets
   * Glue connections

---

## ▶️ Running the Workflow

1. Go to **API Gateway** in AWS Console.
2. Select the resource `archival` → `POST` method.
3. Use the following payload (sample provided in `input_payloads/archival_input_payload.json`):

```json
{
  "SourceSystemId": "INT024_TEST_DATA",
  "TargetSystem": "AWS_Athena",
  "ArchiveType": "Full",
  "Tables": ["EMPLOYEES", "SALARIES"]
}
```

4. This triggers:

   * **Step Function `archival_flow`**
   * Runs **Glue Job (archival)** → archives data into Iceberg
   * Runs **Glue Job (validation)** → validates source vs target
   * Runs **Lambda (report_generation)** → creates a PDF in S3
   * Updates **DynamoDB** with metadata
   * Sends logs to **CloudWatch**

---

## 📊 Outputs

* **S3** → Archived data + PDF reports
* **DynamoDB** → Metadata storage
* **CloudWatch** → Logs for Lambda, Glue, Step Functions
* **API Gateway URL** → Entry point for triggering archival workflow

---

## 🔒 IAM Roles

The following IAM roles are created:

* `lambda_role`
* `glue_role`
* `step_function_role`

Each role has least-privilege access for its respective service.

---

## 🌍 Environments

* **Development** (`dev`)
* **Quality Assurance** (`qa`)
* **Production** (`prod`)

Switch environments using:

```bash
./deploy.sh qa
```

---

## 📝 Notes

* All infrastructure is fully managed by **Terraform**.
* Logs are centralized in **CloudWatch** under `clpidwatch`.
* The project is modular and supports adding new Glue jobs or Lambda functions easily.

---

## 👤 Author

**Gokul R.**

```

---

✅ This is a **ready-to-use README.md** that matches your existing folder structure.  

Do you also want me to include an **architecture diagram (workflow picture)** section in the README, so it looks more like an open-source project?
```
