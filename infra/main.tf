############################################################################
# Infrastructure AWS - projet de détection de fraude
#
# Ce fichier décrit l'état souhaité ; Terraform compare avec
# ce qui existe déjà et crée / modifie uniquement la différence.
############################################################################

# ---------------------------------------------------------------------------
# 1. Configuration de Terraform et du fournisseur (provider) AWS
# ---------------------------------------------------------------------------
terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0, < 7.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile # profil créé avec `aws configure` : aucun secret dans le code

  # Étiquettes ajoutées automatiquement à toutes les ressources
  default_tags {
    tags = {
      project   = var.project
      managedby = "terraform"
    }
  }
}

# ---------------------------------------------------------------------------
# 2. Variables : les paramètres que l'on peut changer sans toucher au code
# ---------------------------------------------------------------------------
variable "region" {
  description = "Région AWS. Paris = données hébergées dans l'UE (RGPD)."
  type        = string
  default     = "eu-west-3"
}

variable "aws_profile" {
  description = "Profil AWS CLI utilisé par Terraform."
  type        = string
  default     = "fraud-deployer"
}

variable "project" {
  description = "Préfixe des ressources."
  type        = string
  default     = "fraud-detection"
}

variable "raw_retention_days" {
  description = "Durée de conservation des données brutes (minimisation RGPD)."
  type        = number
  default     = 90
}

# Lit l'identité AWS courante : sert à rendre le nom du bucket unique au monde
data "aws_caller_identity" "current" {}

locals {
  bucket_name = "${var.project}-${data.aws_caller_identity.current.account_id}"
}

# ---------------------------------------------------------------------------
# 3. Bucket S3 : données brutes (raw/), quarantaine, modèles versionnés
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "data" {
  bucket = local.bucket_name
}

# Interdit toute exposition publique, quoi qu'il arrive
resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Chiffrement au repos (AES-256 géré par AWS)
resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Versioning : un écrasement ou une suppression accidentelle reste récupérable
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Cycle de vie : rétention limitée + nettoyage automatique
resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket     = aws_s3_bucket.data.id
  depends_on = [aws_s3_bucket_versioning.data]

  # Les données brutes (qui contiennent des données personnelles) expirent
  rule {
    id     = "expiration-donnees-brutes"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = var.raw_retention_days
    }
  }

  # Nettoyage général : anciennes versions, envois incomplets, marqueurs orphelins
  rule {
    id     = "nettoyage"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    expiration {
      expired_object_delete_marker = true
    }
  }
}

# Refuse toute connexion non chiffrée (HTTP) : TLS obligatoire
resource "aws_s3_bucket_policy" "tls_only" {
  bucket     = aws_s3_bucket.data.id
  depends_on = [aws_s3_bucket_public_access_block.data]

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "RefuserHTTPNonChiffre"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
        Condition = {
          Bool = { "aws:SecureTransport" = "false" }
        }
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# 4. Utilisateur IAM du pipeline : principe du moindre privilège
#    Il ne peut toucher QU'À ce bucket, et seulement lister / lire / écrire.
#    (Pas de suppression, pas d'autre service AWS.)
# ---------------------------------------------------------------------------
resource "aws_iam_user" "pipeline" {
  name = "${var.project}-pipeline"
}

resource "aws_iam_user_policy" "pipeline_s3" {
  name = "acces-bucket-donnees"
  user = aws_iam_user.pipeline.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListerLeBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.data.arn
      },
      {
        Sid      = "LireEtEcrireLesObjets"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "${aws_s3_bucket.data.arn}/*"
      }
    ]
  })
}

# NB : la clé d'accès de cet utilisateur n'est volontairement PAS créée ici :
# Terraform la stockerait en clair dans son fichier d'état (terraform.tfstate).

# ---------------------------------------------------------------------------
# 5. Sorties : valeurs affichées à la fin et réutilisables par la suite
# ---------------------------------------------------------------------------
output "bucket_name" {
  value = aws_s3_bucket.data.bucket
}

output "region" {
  value = var.region
}

output "pipeline_user" {
  value = aws_iam_user.pipeline.name
}
