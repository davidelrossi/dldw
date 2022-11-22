CREATE_JOB_TITLE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS job_titles (
job_title VARCHAR(100),
language VARCHAR(100),
suspended VARCHAR(100));
"""

COPY_SQL ="""
SELECT aws_s3.table_import_from_s3(
'{}', 'job_title,language,suspended', '(format csv, header true)',
'{}',
'{}',
'us-east-1',
'{{}}','{{}}', '{{}}'
)
"""


COPY_ALL_JOB_TITLES_SQL = COPY_SQL.format(
    "job_titles",
    "datalake2bucket",
    "/work_status.csv"
)


VALIDATE_RESULTS = """
DROP TABLE IF EXISTS job_titles_resume;
CREATE TABLE job_titles_resume AS
SELECT
  t.job_title AS job_title,
  COUNT(t.job_title) AS num_positions
FROM job_titles t
GROUP BY t.job_title;
"""
