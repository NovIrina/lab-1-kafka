## Project structure
    
    .
    ├── data                    
    |   └── test_data.csv                  # Final dataset prepared for analysis and processing     
    ├── model
    |   ├── classifier.joblib              # GradientBoostingClassifier model   
    |   ├── encoder.joblib                 # One-Hot Encoder model
    |   └── scaler.joblib                  # StandardScaler model
    ├── backend
    |   ├── __pycache__
    |   ├── consumer.py                    # Class for working with Kafka consumer
    |   ├── data_collection.py             # File for sending data to Kafka
    |   ├── pipeline.py                    # ML pipeline
    |   ├── producer.py                    # Class for working with Kafka producer
    |   ├──__init__.py
    |   ├──preprocessing.py                # File for data preprocessing
    ├── frontend
    |   ├── visualization.py               # File for visualization
    ├── README.md                          # File with project description
    ├── docker-compose.yaml                # Docker container
    ├── main.py                            # Main executable file of the project
    ├── requirements.txt                   # Project dependencies
    ├── EDA.ipynb                          # Notebook with Exploratory Data Analysis (EDA)
    └── .gitignore                         # File with list of folders ignored by git
    
    
## Dataset
The dataset was created based on data provided by <a href="https://www.kaggle.com/datasets/anandshaw2001/imdb-data"> IMBD </a>, with the main purpose of film rating prediction (classification task). The final dataset contains only a part of the original dataset with preliminary processing of target to classification task. In addition, data analysis (EDA) was performed, which provided insight into the characteristics of the data, that was taking into consideration in the preprocessing stage final dataset.

To download the final dataset use the following <a href="https://drive.google.com/file/d/1UydkxlfkVIlImJlA4WJqNQKSiNX75EVE/view?usp=sharing"> link </a>. After downloading, put the downloaded dataset in the `data` folder.

## Run project
1. Create virtual environment
    ```bash
    python3.10 -m venv venv
    source ./venv/bin/activate
    ```
2. Install required dependencies
    ```bash
    pip install -r requirements.txt
    ```
3. Add PYTHONPATH
    ```bash
    export PYTHONPATH=./backend:$PYTHONPATH
    ```
4. Run project
    ```
    python main.py
    ```

## Demonstration

* Dashboard with F1 score

* Dashboard with the dependence of the predicted rating on the popularity of the film

