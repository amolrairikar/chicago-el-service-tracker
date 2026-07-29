# CloudFront domain that serves the gold/ layer. Use this as the base URL the
# React dashboard fetches gold objects from (e.g. https://<domain>/routes.json).
output "cloudfront_domain_name" {
  description = "Default *.cloudfront.net domain of the gold-layer distribution."
  value       = aws_cloudfront_distribution.gold.domain_name
}

# Distribution ID — needed to issue cache invalidations after a gold refresh.
output "cloudfront_distribution_id" {
  description = "ID of the gold-layer CloudFront distribution (for cache invalidations)."
  value       = aws_cloudfront_distribution.gold.id
}

# Distribution ARN — scope the future gold-writer's cloudfront:CreateInvalidation
# IAM permission to this ARN.
output "cloudfront_distribution_arn" {
  description = "ARN of the gold-layer CloudFront distribution."
  value       = aws_cloudfront_distribution.gold.arn
}
