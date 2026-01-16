# YouTube Conflict Data Analysis

This project performs a comprehensive analysis of YouTube videos related to the Israel-Palestine conflict over a 10-year period (2014-2024). It collects metadata and comments, processes the data using PySpark for sentiment analysis and topic modeling, and generates a detailed PDF report.

## Prerequisites

- **Python 3.8+**
- **Java 8 or 11** (Required for PySpark)
- **Git**

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd projet-bd
    ```

2.  **Create a virtual environment:**
    ```bash
    python3 -m venv venv
    ```

3.  **Activate the virtual environment:**
    - On Linux/macOS:
      ```bash
      source venv/bin/activate
      ```
    - On Windows:
      ```bash
      .\venv\Scripts\activate
      ```

4.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Usage

The project includes a helper script `run.sh` that sets up the environment and runs the main analysis pipeline.

1.  **Run the analysis:**
    ```bash
    ./run.sh
    ```

    *Note: The `run.sh` script currently includes a configured API key and sets up the necessary PYTHONPATH and PySpark environment variables.*

2.  **View the Results:**
    After the script completes, the final report will be generated at:
    `data/final_comprehensive_report.pdf`

## Project Structure

- `src/`: Source code for collection, processing, analysis, and reporting.
- `data/`: Directory for raw data, processed data, and generated reports.
- `run.sh`: Main execution script.
- `requirements.txt`: Python dependencies.
