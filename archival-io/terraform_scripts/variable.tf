
variable "environment" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "archival_flow_trigger_lambda_function_name" {
  description = "Name of the Archive Lambda function"
  type        = string
}

variable "report_generation_lambda_function_name" {
  description = "Name of the Validation Lambda function"
  type        = string
}

variable "lambda_layer_name" {
  description = "Name of the Lambda layer"
  type        = string
}

variable "lambda_role_name" {
  description = "IAM Role name for Lambda"
  type        = string
}

variable "glue_role_name" {
  description = "IAM Role name for Glue Jobs"
  type        = string
}

variable "step_function_role_name" {
  description = "IAM Role name for Step Function"
  type        = string
}

variable "lambda_policy_name" {
  description = "IAM Role name for Lambda"
  type        = string
}

variable "glue_policy_name" {
  description = "IAM Role name for Glue Jobs"
  type        = string
}

variable "step_function_policy_name" {
  description = "IAM Role name for step function policy Jobs"
  type        = string
}

variable "archival_flow_step_function_name" {
  description = "Glue job name for archival"
  type        = string
}

variable "archival_glue_job_name" {
  description = "Glue job name for archival"
  type        = string
}

variable "purge_glue_job_name" {
  description = "Glue job name for purge"
  type        = string
}

variable "validation_glue_job_name" {
  description = "Glue job name for purge"
  type        = string
}

variable "mysql_connection_name" {
  description = "Glue data connectin ndame for MySQL"
  type        = string
}

variable "postgres_connection_name" {
  description = "Glue data connectin ndame for Postgres"
  type        = string
}

variable "sqlserver_connection_name" {
  description = "Glue data connectin ndame for SqlServer"
  type        = string
}

variable "oracle_connection_name" {
  description = "Glue data connectin ndame for Oracle"
  type        = string
}

variable "archive_dynamodb_table_name" {
  description = "DynamoDB table name for archive"
  type        = string
}

variable "purge_dynamodb_table_name" {
  description = "DynamoDB table name for purge"
  type        = string
}

variable "apigateway_name" {
  description = "Name of the API Gateway REST API"
  type        = string
  default     = "archival"
}

variable "apigateway_resource_path" {
  description = "Resource path for API Gateway"
  type        = string
  default     = "archival"
}

variable "apigateway_stage_name" {
  description = "API Gateway deployment stage name"
  type        = string
  default     = "dev"
}
