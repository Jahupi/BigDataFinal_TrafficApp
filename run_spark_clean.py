#!/usr/bin/env python
"""
Wrapper script to run spark_analysis.py and suppress annoying Spark shutdown errors
"""
import subprocess
import sys

# Run the spark analysis script
result = subprocess.run(
    [sys.executable, "spark_analysis.py"],
    capture_output=True,
    text=True
)

# Print stdout normally
print(result.stdout, end='')

# Filter stderr to only show relevant errors (not temp file cleanup)
stderr_lines = result.stderr.split('\n')
filtered_errors = [
    line for line in stderr_lines 
    if line and not ('SparkEnv' in line and 'Exception while deleting' in line)
    and not ('ShutdownHookManager' in line and 'Exception while deleting' in line)
    and not ('java.io.IOException: Failed to delete' in line)
    and not ('.jar' in line and 'userFiles' in line)
    and not ('NoSuchFileException' in line and 'pyspark' in line)
]

if filtered_errors:
    print('\n'.join(filtered_errors), file=sys.stderr)

sys.exit(result.returncode)
