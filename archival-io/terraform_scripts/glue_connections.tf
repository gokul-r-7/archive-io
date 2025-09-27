/*
resource "aws_glue_connection" "mysql_connection" {
  name = "${var.mysql_connection_name}-${var.environment}"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:mysql://mysql.cyzimay4eebo.us-east-1.rds.amazonaws.com:3306/mysql_database"
    USERNAME            = "mysql_username"
    PASSWORD            = "mysql_password"
  }
  physical_connection_requirements {
    subnet_id              = "subnet-021311747b53c6466"
    security_group_id_list = ["sg-0cc9e48ee3089e1c5"]
  }
  connection_type = "JDBC"
}
*/

resource "aws_glue_connection" "postgres_connection" {
  name = "${var.postgres_connection_name}-${var.environment}"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:postgresql://dpg-d3565tili9vc739hjb6g-a.virginia-postgres.render.com:5432/postgres_databse_eeql"
    USERNAME            = "postgresusername"
    PASSWORD            = "postgrespassword"
  }
  /*physical_connection_requirements {
    subnet_id              = "subnet-0f480ff10a7fcfdf6"
    security_group_id_list = ["sg-0cc9e48ee3089e1c5"]
  }*/
  connection_type = "JDBC"
}

/*
resource "aws_glue_connection" "sqlserver_connection" {
  name = "${var.sqlserver_connection_name}-${var.environment}"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:sqlserver://sqlserver.cyzimay4eebo.us-east-1.rds.amazonaws.com:1433;database=sqlserver_database"
    USERNAME            = "sql_username"
    PASSWORD            = "sql_password"
  }
  physical_connection_requirements {
    subnet_id              = "subnet-021311747b53c6466"
    security_group_id_list = ["sg-0cc9e48ee3089e1c5"]
  }
  connection_type = "JDBC"
}

resource "aws_glue_connection" "oracle_connection" {
  name = "${var.oracle_connection_name}-${var.environment}"
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:oracle://oracle.cyzimay4eebo.us-east-1.rds.amazonaws.com:1521/ORCL"
    USERNAME            = "oracle_username"
    PASSWORD            = "oracle_password"
  }
  physical_connection_requirements {
    subnet_id              = "subnet-0f480ff10a7fcfdf6"
    security_group_id_list = ["sg-0cc9e48ee3089e1c5"]
  }
  connection_type = "JDBC"
}
*/