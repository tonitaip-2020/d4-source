# Source code analysis

Analyzes source code files for data access methods, i.e., how software applications connect to the database. Limited to C, C++, C#, Java, Python, JavaScript, Ruby, PHP and Go.

## Contents of this repository:

- `analysis_*.py` = files for analyzing the repositories of different languages.
- `create_summary.py` = for summarizing the .csv outputs.
- `summaries.txt` = data summaries output by `create_summaries.py`.
- (raw analysis data are not included due to their size)

## Usage

1. Acquire the dataset at https://huggingface.co/datasets/codeparrot/github-code (downloading the dataset is the preferred method in the files of this repository, but you can also use a data stream to save space and bandwidth, as the dataset is several hundred GBs).
2. Run the .py file(s) depending on which language's data access methods you want to analyze.
3. Results are saved in a .csv file.

## Script execution times (minutes)

- C:            29
- C#:           20
- C++:          15
- Go:           17
- Java:         64
- JavaScript:   31
- PHP:          39
- Python:       103
- Ruby:         9
