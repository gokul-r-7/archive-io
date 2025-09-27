resource "aws_cloudwatch_log_group" "step_function_logs" {
  name              = "/aws/vendedlogs/states/${var.archival_flow_step_function_name}-${var.environment}"
  retention_in_days = 14
}

resource "aws_sfn_state_machine" "archival_flow" {
  name     = "${var.archival_flow_step_function_name}-${var.environment}"
  role_arn = aws_iam_role.step_function_role.arn

  definition = file("${path.module}/state_machine_definition/archival_flow.json")

  type = "STANDARD"

  logging_configuration {
    include_execution_data = true
    level                  = "ALL"
    log_destination        = "${aws_cloudwatch_log_group.step_function_logs.arn}:*"
  }

  depends_on = [
    aws_iam_role_policy_attachment.step_function_attachment
  ]
}
