terraform {
  backend "s3" {
    bucket = "cde-capstone-olalekan-statefile"
    key    = "dev/dev.tfstate"
    use_lockfile = true
    region = "us-east-1"
  }
}
