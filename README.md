# Source code analysis

Analyzes source code files for data access methods, i.e., how software applications connect to the database. Limited to C, C++, C#, Java, Python, JavaScript, Ruby, PHP and Go.

## Usage

1. Acquire the dataset at https://huggingface.co/datasets/codeparrot/github-code (downloading the dataset is the preferred method in the files of this repository, but you can also use a data stream to save space and bandwidth).
2. Run the .py file(s) depending on which language's data access methods you want to analyze.
3. Results are saved in a .csv file, some of which are too large for Excel to open. Use the .sql file to import the .csv files to PostgreSQL. 
