------------------------------------------------------------------------------------------------------------------
-- Create a Cloud Vision Model
------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE MODEL
  `PROJECT_ID.object_tables_dataset.vision_model` REMOTE
WITH CONNECTION `projects/ajmalaziz-814-20250326021733/locations/us/connections/vertex_connection` 
OPTIONS (REMOTE_SERVICE_TYPE = 'CLOUD_AI_VISION_V1');


------------------------------------------------------------------------------------------------------------------
-- 🔎 We create an object table (we support PDFs, Images, Audio, etc.)
------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE
  `object_tables_dataset.images_object_table`
WITH CONNECTION `projects/ajmalaziz-814-20250326021733/locations/us/connections/vertex_connection` 
OPTIONS( object_metadata = 'SIMPLE',
    uris = 
    [
      'gs://object-tables-dataset-demo/file_1.png',
      'gs://object-tables-dataset-demo/file_2.png',
      'gs://object-tables-dataset-demo/file_3.png',
      'gs://object-tables-dataset-demo/file_4.png',
      'gs://object-tables-dataset-demo/file_5.png',
      'gs://object-tables-dataset-demo/file_6.png',
      'gs://object-tables-dataset-demo/file_7.png',
      'gs://object-tables-dataset-demo/file_8.png' 
  ] 
);


------------------------------------------------------------------------------------------------------------------
-- We can leverage Cloud Vision AI directly from SQL and perform simple tasks like logo detection or face detection
------------------------------------------------------------------------------------------------------------------
SELECT
  t.uri,
  JSON_EXTRACT_SCALAR(logo, '$.description') AS located_logo
FROM
  ML.ANNOTATE_IMAGE( MODEL `object_tables_dataset.vision_model`,
    TABLE `object_tables_dataset.images_object_table`,
    STRUCT(['logo_detection'] AS vision_features) ) AS t,
  UNNEST(JSON_EXTRACT_ARRAY(t.ml_annotate_image_result, '$.logo_annotations')) AS logo;


------------------------------------------------------------------------------------------------------------------
-- 🔎 We can also use Gemini's vision capabilities to describe the images for more complicated tasks
------------------------------------------------------------------------------------------------------------------
SELECT
  uri,
  ml_generate_text_llm_result
FROM
  ML.GENERATE_TEXT( MODEL `object_tables_dataset.gemini_2_5_flash_preview`,
    TABLE `object_tables_dataset.images_object_table`,
    STRUCT( 'What brand of alcohol is shown in this image?:' AS PROMPT,
      8192 AS max_output_tokens,
      TRUE AS FLATTEN_JSON_OUTPUT ) );

------------------------------------------------------------------------------------------------------------------
-- 🔎 More subjective tasks now that require more advanced reasoning.
------------------------------------------------------------------------------------------------------------------
SELECT
  uri,
  ml_generate_text_llm_result
FROM
  ML.GENERATE_TEXT( MODEL `object_tables_dataset.gemini_2_5_flash_preview`,
    TABLE `object_tables_dataset.images_object_table`,
    STRUCT( 'Is this Wine or Beer?:' AS PROMPT,
      8192 AS max_output_tokens,
      TRUE AS FLATTEN_JSON_OUTPUT ) );


