######################################################
#Configurações Cloud Storage
######################################################
resource "google_storage_bucket" "my_storage" {
  name          = local.final_bucket_name
  location      = var.location
  force_destroy = true

  labels = {
    produto = local.project_name
  }
}

resource "null_resource" "cf_code_zip" {
  triggers = {
    on_version_change = var.project_version
  }

  provisioner "local-exec" {
    command = "bash scripts/using-local-project.sh ${var.project_version} ${local.final_bucket_name}"
  }

  depends_on = [google_storage_bucket.my_storage]
}

resource "google_storage_bucket" "dataset_example_pipeline_penguin" {
  name          = "${local.project_name}-dataset_example_pipeline_penguin"
  location      = var.location
  force_destroy = true

  labels = {
    produto = local.project_name
  }
}

resource "null_resource" "dataset_to_storage" {
  triggers = {
    on_version_change = var.project_version
  }

  provisioner "local-exec" {
    command = "bash scripts/data-to-storage.sh ${local.project_name}-dataset_example_pipeline_penguin"
  }

  depends_on = [google_storage_bucket.dataset_example_pipeline_penguin]
}

# ######################################################
# #Configurações BigQuery
# ######################################################
resource "google_bigquery_dataset" "penguinpipeline" {
  dataset_id                  = "penguinpipeline"
  friendly_name               = "penguinpipeline"
  description                 = "This is a test penguin pipeline test"
  location                    = var.location
  default_table_expiration_ms = 3600000

  labels = {
    produto = local.project_name
  }

}

resource "google_bigquery_table" "example_pipeline_penguin" {
  dataset_id = "penguinpipeline"
  table_id   = "example-penguin-pipeline"

  labels = {
    produto = local.project_name
  }

  schema = <<EOF
[
  {
    "name": "event_timestamp",
    "type": "TIMESTAMP",
    "mode": "NULLABLE"
  },{
    "name": "event_id",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "event_type",
    "type": "STRING",
    "mode": "NULLABLE"
  },{
    "name": "page_url",
    "type": "STRING",
    "mode": "NULLABLE"
  }
]
EOF

  depends_on = [google_bigquery_dataset.penguinpipeline]

}

resource "google_bigquery_job" "job_load_example_data" {
  job_id     = "job_load_example_data"
  location   = var.location

  labels = {
    produto = local.project_name
  }

  load {
    source_uris = [
      "gs://${local.project_name}-dataset_example_pipeline_penguin/example_data.csv",
    ]

    destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.penguinpipeline.dataset_id
      table_id   = google_bigquery_table.example_pipeline_penguin.table_id
    }

    skip_leading_rows = 1
    schema_update_options = ["ALLOW_FIELD_RELAXATION", "ALLOW_FIELD_ADDITION"]

    write_disposition = "WRITE_APPEND"
    autodetect = true
  }

  depends_on = [google_bigquery_table.example_pipeline_penguin]

}

# ##################################
# #Configurações Cloud Function
# ##################################
resource "google_cloudfunctions_function" "pp_function" {
  project               = var.project_id
  name                  = local.cf_name
  description           = "CF pipeline penguin"
  runtime               = "python39"
  service_account_email = var.service_account_email
  region                = var.region
  available_memory_mb   = 512
  timeout               = 120
  source_archive_bucket = google_storage_bucket.my_storage.name
  source_archive_object = "${var.project_version}.zip"
  trigger_http          = true
  entry_point           = local.cf_entry_point

  environment_variables = {
    PROJECT_BUCKET_GCS = local.final_bucket_name
  }
  depends_on = [null_resource.cf_code_zip]
}

# # # IAM entry for all users to invoke the function
resource "google_cloudfunctions_function_iam_member" "invoker" {
  project        = var.project_id
  region         = google_cloudfunctions_function.pp_function.region
  cloud_function = google_cloudfunctions_function.pp_function.name

  role   = "roles/cloudfunctions.invoker"
  member = "allUsers"
}


# ##################################
# #Configurações Scheduler
# ##################################
resource "google_cloud_scheduler_job" "scheduler" {
  name             = "${var.project_prefix}-scheduler"
  description      = "Scheduler HTTP trigger for Pipeline Penguin"
  schedule         = "0 15 * * *"
  time_zone        = "America/Sao_Paulo"
  attempt_deadline = "320s"
  depends_on       = [google_cloudfunctions_function.pp_function]

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "GET"
    uri         = google_cloudfunctions_function.pp_function.https_trigger_url
  }
}