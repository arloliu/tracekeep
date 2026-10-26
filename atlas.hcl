# Atlas versioned migrations, one directory per engine, from the one schema.hcl (docs/specs/tracekeep/catalog.md §7.4).
# Run through scripts/catalog.sh (make migrate-diff, make migrate-check), which starts the MariaDB dev database.

variable "engine" {
  type = string
}

locals {
  types = {
    sqlite  = { uuid = "blob", hash = "blob", header = "blob", bytes = "blob" }
    mariadb = { uuid = "binary(16)", hash = "binary(32)", header = "binary(44)", bytes = "mediumblob" }
  }
  dev = {
    sqlite  = "sqlite://dev?mode=memory"
    mariadb = getenv("TRACEKEEP_ATLAS_MARIADB_DEV")
  }
}

data "hcl_schema" "catalog" {
  path = "schema.hcl"
  vars = local.types[var.engine]
}

env "catalog" {
  src = data.hcl_schema.catalog.url
  dev = local.dev[var.engine]
  migration {
    dir = "file://migrations/${var.engine}"
  }
}
