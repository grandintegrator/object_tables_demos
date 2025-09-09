# Demo of BQ Multimodal Capabilities (images and PDF)

Prerequisites: 
1. Please upload the data directory (without the data folder) into GCS and copy the uri.
2. Create a Vertex AI Connection and copy the Connection ID. Mine was: `projects/ajmalaziz-814-20250326021733/locations/us/connections/vertex_connection`

## PDF Parsing
Demos unstructured (PDF on GCS) to structured (invoices). Script is in `pdf_parsing.sql`.

## Image Understanding
Uses Cloud Vision AI for logo detection and Gemini for more subjective questions. Script is in `image_parsing.sql`.

