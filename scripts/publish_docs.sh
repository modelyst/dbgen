#!/bin/sh
# Invalidate
pdocs as_markdown dbgen -o ./docs/reference -t ./docs/templates --overwrite 
mkdocs build
aws s3 --profile $1 sync ./site s3://www.dbgen.modelyst.com --region us-west-1
aws cloudfront --profile $1 create-invalidation --distribution-id E1ET585EUZ231U --paths "/*"