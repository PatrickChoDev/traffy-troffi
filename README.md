# TRAFFY TROFFI

A data science project for citizen complaint management by government platforms using PySpark, Streamlit, and machine learning pipelines.

## Description

TRAFFY TROFFI (Transparent Responsive Application For Feedback & Yielding) is a data science semester project that focuses on analyzing and visualizing citizen complaints submitted to government platforms. The project leverages PySpark for data processing, Seaborn for visualization, and incorporates machine learning pipelines for complaint categorization and response time prediction.

Key features:
- Processing large-scale complaint datasets with PySpark
- Interactive complaint visualization dashboard with Streamlit
- Predictive analytics using machine learning models for issue prioritization
- Complaint pattern analysis and public service insights generation

## Setup

### Prerequisites

- Python 3.12 or higher
- [Poetry](https://python-poetry.org/) (dependency management)
- [mise](https://mise.jdx.dev/) (optional - for Python version management)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/PatrickChoDev/traffy-troffi.git
   cd traffy-troffi
   ```

2. Set up the Python environment using mise (optional):
   ```bash
   mise install
   ```

3. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

4. Activate the virtual environment:
   ```bash
   poetry shell
   ```

### Running the Application

To start the Streamlit dashboard:
```bash
streamlit run traffy_troffi/app.py
```

## Development

### To install development dependencies:
```bash
poetry install --with dev
```
This will install additional tools like Jupyter notebooks for development and experimentation.

### To bootstrap Postgres, S3 storage, and Apache Spark
```bash
docker compose up # use -d for daemon mode
```

### To run Dagster dashboard
```bash
dagster dev
```
*Notes: Leaves Dagster components commented in compose.yaml except production env*


