# Source code analysis

Analyzes source code files for data access methods, i.e., how software applications connect to the database. Limited to C, C++, C#, Java, Python, JavaScript, Ruby, PHP and Go.

## Contents of this repository:

- `analyze.py` = for analyzing the repositories of different languages.
- `create_summary.py` = for summarizing the .csv outputs.
- `summary.txt` = data summaries output by `create_summaries.py`.
- (raw analysis data are not included due to their size)

## Usage

1. Acquire the dataset at https://huggingface.co/datasets/codeparrot/github-code (downloading the dataset is the preferred method in the files of this repository, but you can also use a data stream to save space and bandwidth, as the dataset is several hundred GBs).

In Powershell:

```
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install datasets huggingface_hub
```

If PowerShell blocks activation on Windows, use a process-scoped execution policy change for just the current shell session:

```
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Download the Parquet files:

```
@'
from huggingface_hub import snapshot_download

snapshot_download(
    repo_id="codeparrot/github-code",
    repo_type="dataset",
    local_dir=".",
    allow_patterns=["data/train-0*-of-01126.parquet"],
)
'@ | python -
```

   
3. Run the analyze.py with parameters depending on which language's data access methods you want to analyze.

If you downloaded the files (step #2 above), run:

```
$env:HF_DATASETS_OFFLINE = "1"
```

Run the script for the language you want to, e.g. (this will scan the Parquet files, and not create Arrow files to save disk space):

```
python .\analyze.py ruby
```

4. Results are saved in a .csv file.

5. Run create_summaries to generate summaries from the .csv file(s):

```
python .\create_summaries.py .
```

