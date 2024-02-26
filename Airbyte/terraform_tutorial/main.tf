terraform {
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "0.4.1"
    }
  }
}

provider "airbyte" {
  # Configuration options
  bearer_auth = var.api_key
}

resource "airbyte_source_stripe" "my_source_stripe" {
  configuration = {
    source_type          = "stripe"
    account_id           = ""
    client_secret        = var.stripe_key
    start_date           = "2024-03-03T00:00:00Z"
    lookback_window_days = 0
    slice_range          = 365
  }
  name         = "[TEST] SourceStripe"
  workspace_id = var.workspace_id
}

resource "airbyte_destination_bigquery" "my_destination_bigquery" {
  configuration = {
    big_query_client_buffer_size_mb = 15
    credentials_json                = var.credentials
    dataset_id                      = "test_ds"
    dataset_location                = "US"
    destination_type                = "bigquery"
    project_id                      = "sandbox-123"
    transformation_priority         = "batch"
  }
  name         = "[TEST] DestinationBigQuery"
  workspace_id = var.workspace_id
}

resource "airbyte_connection" "stripe_to_bigquery" {
  name           = "Stripe to BigQuery"
  source_id      = airbyte_source_stripe.my_source_stripe.source_id
  destination_id = airbyte_destination_bigquery.my_destination_bigquery.destination_id

}