#!/bin/sh

#   Copyright 2021 Modelyst LLC
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Invalidate
pdocs as_markdown dbgen -o ./docs/reference -t ./docs/templates --overwrite
mkdocs build
aws s3 --profile $1 sync ./site s3://www.dbgen.modelyst.com --region us-west-1
aws cloudfront --profile $1 create-invalidation --distribution-id E1ET585EUZ231U --paths "/*"
