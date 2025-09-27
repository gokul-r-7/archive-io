resource "aws_dynamodb_table" "archive_tf" {
  name         = "${var.archive_dynamodb_table_name}-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "purge_tf" {
  name         = "${var.purge_dynamodb_table_name}-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "job_id"

  attribute {
    name = "job_id"
    type = "S"
  }
}
