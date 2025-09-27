# Shared Lambda Layer
resource "aws_lambda_layer_version" "shared_dependencies" {
  filename            = "${path.module}/lambda_layer/lambda_layer.zip"
  layer_name          = var.lambda_layer_name
  compatible_runtimes = ["python3.9"]
  source_code_hash    = filebase64sha256("${path.module}/lambda_layer/lambda_layer.zip")
}

# Upload Lambda Layer ZIP to S3
resource "aws_s3_object" "lambda_layer_code" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "lambda_layer/lambda_layer.zip"
  source = "${path.module}/lambda_layer/lambda_layer.zip"
  etag   = filemd5("${path.module}/lambda_layer/lambda_layer.zip")

  lifecycle {
    create_before_destroy = true
  }
}

# Upload Lambda Function Zips to S3
resource "aws_s3_object" "archival_flow_trigger_lambda_code" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "lambda_function/archival_flow_trigger_lambda.zip"
  source = "${path.module}/lambda_functions/archival_flow_trigger/lambda_function.zip"
  etag   = filemd5("${path.module}/lambda_functions/archival_flow_trigger/lambda_function.zip")

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_s3_object" "report_generation_lambda_code" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "lambda_function/report_generation_lambda.zip"
  source = "${path.module}/lambda_functions/report_generation/lambda_function.zip"
  etag   = filemd5("${path.module}/lambda_functions/report_generation/lambda_function.zip")

  lifecycle {
    create_before_destroy = true
  }
}

# Archival Flow Trigger Lambda Function
resource "aws_lambda_function" "archival_flow_trigger" {
  function_name = "${var.archival_flow_trigger_lambda_function_name}-${var.environment}"
  role          = aws_iam_role.lambda_exec_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300

  s3_bucket        = aws_s3_bucket.my_bucket.bucket
  s3_key           = aws_s3_object.archival_flow_trigger_lambda_code.key
  source_code_hash = filebase64sha256("${path.module}/lambda_functions/archival_flow_trigger/lambda_function.zip")

  layers = [aws_lambda_layer_version.shared_dependencies.arn]

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_policy,
    aws_s3_object.archival_flow_trigger_lambda_code,
    aws_s3_object.lambda_layer_code
  ]
}

# Report Generation Lambda Function
resource "aws_lambda_function" "report_generation" {
  function_name = "${var.report_generation_lambda_function_name}-${var.environment}"
  role          = aws_iam_role.lambda_exec_role.arn
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.9"
  timeout       = 300

  s3_bucket        = aws_s3_bucket.my_bucket.bucket
  s3_key           = aws_s3_object.report_generation_lambda_code.key
  source_code_hash = filebase64sha256("${path.module}/lambda_functions/report_generation/lambda_function.zip")

  layers = [aws_lambda_layer_version.shared_dependencies.arn]

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_policy,
    aws_s3_object.report_generation_lambda_code,
    aws_s3_object.lambda_layer_code
  ]
}
