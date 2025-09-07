# !pip install xgboost
# !pip install hyperopt
# !pip install dotenv
# !pip install dbutils
import logging
logging.getLogger('py4j').setLevel(logging.CRITICAL)
from jobs import regression_training  # your Dagster job 


def regression_train():

    result = regression_training()
    if result:
        print("Job executed successfully")
    else:
        print("Job failed")

if __name__ == "__main__":
    regression_train()
