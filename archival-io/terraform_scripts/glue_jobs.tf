resource "aws_s3_object" "archival_glue_script" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "glue_scripts/archival.py"
  source = "${path.module}/glue_scripts/archival.py"
}

resource "aws_s3_object" "purge_glue_script" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "glue_scripts/purge.py"
  source = "${path.module}/glue_scripts/purge.py"
}

resource "aws_s3_object" "validation_glue_script" {
  bucket = aws_s3_bucket.my_bucket.bucket
  key    = "glue_scripts/validation.py"
  source = "${path.module}/glue_scripts/validation.py"
}





resource "aws_glue_job" "archival_job_tf" {
  name     = "${var.archival_glue_job_name}-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.my_bucket.bucket}/${aws_s3_object.archival_glue_script.key}"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480

  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = {
    "--enable-continuous-log-filter" = "true"
    "--enable-continuous-log"        = "true"
    "--enable-metrics"               = "true"
    "--job-language"                 = "python"
  }

  depends_on = [aws_s3_object.archival_glue_script]
}

resource "aws_glue_job" "purge_job_tf" {
  name     = "${var.purge_glue_job_name}-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.my_bucket.bucket}/${aws_s3_object.purge_glue_script.key}"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480

  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = {
    "--enable-continuous-log-filter" = "true"
    "--enable-continuous-log"        = "true"
    "--enable-metrics"               = "true"
    "--job-language"                 = "python"
  }

  depends_on = [aws_s3_object.purge_glue_script]
}


resource "aws_glue_job" "validation_job_tf" {
  name     = "${var.validation_glue_job_name}-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.my_bucket.bucket}/${aws_s3_object.validation_glue_script.key}"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 480

  execution_property {
    max_concurrent_runs = 4
  }

  default_arguments = {
    "--enable-continuous-log-filter" = "true"
    "--enable-continuous-log"        = "true"
    "--enable-metrics"               = "true"
    "--job-language"                 = "python"
  }

  depends_on = [aws_s3_object.validation_glue_script]
}
