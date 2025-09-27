resource "aws_api_gateway_rest_api" "archival_api" {
  name        = "${var.apigateway_name}-${var.environment}"
  description = "API Gateway REST API for archival"
}

resource "aws_api_gateway_resource" "archival_resource" {
  rest_api_id = aws_api_gateway_rest_api.archival_api.id
  parent_id   = aws_api_gateway_rest_api.archival_api.root_resource_id
  path_part   = var.apigateway_resource_path
}

resource "aws_api_gateway_method" "post_archival" {
  rest_api_id   = aws_api_gateway_rest_api.archival_api.id
  resource_id   = aws_api_gateway_resource.archival_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_lambda_permission" "apigw_lambda" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.archival_flow_trigger.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.archival_api.execution_arn}/*/POST/${var.apigateway_resource_path}"
}

resource "aws_api_gateway_integration" "post_archival_integration" {
  rest_api_id             = aws_api_gateway_rest_api.archival_api.id
  resource_id             = aws_api_gateway_resource.archival_resource.id
  http_method             = aws_api_gateway_method.post_archival.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.archival_flow_trigger.invoke_arn
}

resource "aws_api_gateway_deployment" "archival_deployment" {
  depends_on = [
    aws_api_gateway_integration.post_archival_integration,
  ]

  rest_api_id = aws_api_gateway_rest_api.archival_api.id
  # Remove stage_name here!
}

resource "aws_api_gateway_stage" "dev_stage" {
  deployment_id = aws_api_gateway_deployment.archival_deployment.id
  rest_api_id   = aws_api_gateway_rest_api.archival_api.id
  stage_name    = var.apigateway_stage_name
}
