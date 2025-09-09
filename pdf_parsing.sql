------------------------------------------------------------------------------------------------------------------
-- We create an object table (we support PDFs, Images, Audio, etc.)
------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE `edg_drinks.invoices_object_tables`
WITH CONNECTION `projects/ajmalaziz-814-20250326021733/locations/us/connections/vertex_connection`
OPTIONS(
  object_metadata = 'SIMPLE',
  uris = [
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_1.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_2.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_3.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_4.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_5.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_6.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_7.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_8.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_9.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_10.pdf',
    'gs://object-tables-dataset-demo/invoices/IN-EDG-2025-00_11.pdf'
  ]);

------------------------------------------------------------------------------------------------------------------
-- We now chunk the PDFs (if needed) and parse out the JSON response
------------------------------------------------------------------------------------------------------------------
CREATE or REPLACE TABLE `object_tables_dataset.chunked_pdfs` AS (
  SELECT * FROM ML.PROCESS_DOCUMENT(
  MODEL `ajmalaziz-814-20250326021733.object_tables_dataset.parser_model`,
  TABLE `object_tables_dataset.invoices_object_tables`,
  PROCESS_OPTIONS => (JSON '{"layout_config": {"chunking_config": {"chunk_size": 250}}}')
  )
);

CREATE OR REPLACE TABLE `object_tables_dataset.parsed_with_chunks_pdf` AS (
SELECT
  uri,
  JSON_EXTRACT_SCALAR(json , '$.content') AS content,
FROM `object_tables_dataset.chunked_pdfs`, UNNEST(JSON_EXTRACT_ARRAY(ml_process_document_result.chunkedDocument.chunks, '$')) json
);


------------------------------------------------------------------------------------------------------------------
-- Let's now see our parsed PDFs
------------------------------------------------------------------------------------------------------------------
select * from `object_tables_dataset.parsed_with_chunks_pdf` limit 10;



----------------------------------------------------------------------------------------------------------------
-- We can now combine the outputs to get the entire PDF content per row.
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `object_tables_dataset.final_combined_pdf`
AS SELECT
  uri,
  STRING_AGG(content, '\n') AS merged_content
FROM
  `object_tables_dataset.parsed_with_chunks_pdf`
GROUP BY
  uri;

----------------------------------------------------------------------------------------------------------------
-- Now we can run Gemini to summarise the document (if needed)
----------------------------------------------------------------------------------------------------------------
SELECT
  uri, ml_generate_text_llm_result
FROM
  ML.GENERATE_TEXT(
    MODEL `edg_drinks.gemini_2_5_flash_preview`,
    (
      SELECT
        source_table.*,
        CONCAT('Summarize this invoice, what was the total and the ID?: ', source_table.merged_content) AS prompt
      FROM
        `edg_drinks.final_combined_pdf` AS source_table
    ),
    STRUCT(8192 AS max_output_tokens, TRUE AS flatten_json_output)
  );


----------------------------------------------------------------------------------------------------------------
-- 📋 Finally, we can get structured tables out for our invoices
----------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `object_tables_dataset.invoices` AS (
  SELECT
    customer, 
    invoice_id,
    amount_due
  FROM
    AI.GENERATE_TABLE( 
      MODEL `object_tables_dataset.gemini_2_5_flash_preview`,
      (
        SELECT *, CONCAT("What was the invoice id and the amount due, and the customer?: ", merged_content) AS PROMPT 
        FROM `object_tables_dataset.final_combined_pdf`
      ),
      STRUCT(
        "invoice_id STRING, amount_due FLOAT64, customer STRING" AS output_schema,
        8192 AS max_output_tokens)
    )
);

----------------------------------------------------------------------------------------------------------------
-- 🚀 Finally, we can see the invoices table
----------------------------------------------------------------------------------------------------------------
SELECT * FROM `object_tables_dataset.invoices`
ORDER BY invoice_id



