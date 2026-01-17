variable "region" {
  type        = string
  description = "AWS region"
  default     = "eu-west-1"
}

variable "instance_type" {
  type        = string
  description = "EC2 instance type"
  default     = "t3.small"
}

variable "root_volume_gb" {
  type        = number
  description = "Root EBS volume size in GB"
  default     = 30
}

variable "key_name" {
  type        = string
  description = "EC2 key pair name for SSH"
}

variable "ssh_cidr" {
  type        = string
  description = "CIDR allowed to SSH and access console (example: 1.2.3.4/32)"
}

variable "repo_url" {
  type        = string
  description = "Git URL for pm-market-data"
}

variable "repo_branch" {
  type        = string
  description = "Git branch to deploy"
  default     = "main"
}

variable "symbols" {
  type        = list(string)
  description = "Symbols to run followers for"
  default     = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
}

variable "instance_name" {
  type        = string
  description = "EC2 Name tag"
  default     = "pm-market-data"
}

variable "subnet_id" {
  type        = string
  description = "Subnet ID (optional; defaults to first subnet in default VPC)"
  default     = ""
}

variable "tags" {
  type        = map(string)
  description = "Tags to apply to AWS resources"
  default = {
    Project = "pm-market-data"
  }
}

variable "ssh_key_path" {
  type        = string
  description = "Path used in the SSH command output"
  default     = "~/.ssh/id_rsa"
}
